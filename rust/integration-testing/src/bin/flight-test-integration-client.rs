// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use arrow_integration_testing::{
    read_json_file, ArrowFile, AUTH_PASSWORD, AUTH_USERNAME,
};

use arrow::array::ArrayRef;
use arrow::datatypes::SchemaRef;
use arrow::ipc::{self, reader, writer};
use arrow::record_batch::RecordBatch;

use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::{
    flight_descriptor::DescriptorType, BasicAuth, HandshakeRequest, Location, Ticket,
};
use arrow_flight::{utils::flight_data_to_arrow_batch, FlightData, FlightDescriptor};

use clap::{App, Arg};
use futures::{channel::mpsc, sink::SinkExt, stream, StreamExt};
use prost::Message;
use tonic::{metadata::MetadataValue, Request, Status, Streaming};

use std::sync::Arc;

type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
type Result<T = (), E = Error> = std::result::Result<T, E>;

type Client = FlightServiceClient<tonic::transport::Channel>;

#[tokio::main]
async fn main() -> Result {
    let matches = App::new("rust flight-test-integration-client")
        .arg(Arg::with_name("host").long("host").takes_value(true))
        .arg(Arg::with_name("port").long("port").takes_value(true))
        .arg(Arg::with_name("path").long("path").takes_value(true))
        .arg(
            Arg::with_name("scenario")
                .long("scenario")
                .takes_value(true),
        )
        .get_matches();

    let host = matches.value_of("host").expect("Host is required");
    let port = matches.value_of("port").expect("Port is required");

    match matches.value_of("scenario") {
        Some("middleware") => middleware_scenario(host, port).await?,
        Some("auth:basic_proto") => auth_basic_proto_scenario(host, port).await?,
        Some(scenario_name) => unimplemented!("Scenario not found: {}", scenario_name),
        None => {
            let path = matches
                .value_of("path")
                .expect("Path is required if scenario is not specified");
            integration_test_scenario(host, port, path).await?;
        }
    }

    Ok(())
}

async fn middleware_scenario(host: &str, port: &str) -> Result {
    let url = format!("http://{}:{}", host, port);
    let conn = tonic::transport::Endpoint::new(url)?.connect().await?;
    let mut client = FlightServiceClient::with_interceptor(conn, middleware_interceptor);

    let mut descriptor = FlightDescriptor::default();
    descriptor.set_type(DescriptorType::Cmd);
    descriptor.cmd = b"".to_vec();

    // This call is expected to fail.
    let resp = client.get_flight_info(Request::new(descriptor.clone()));

    let resp = resp.await;

    match resp {
        Ok(_) => return Err(Box::new(Status::internal("Expected call to fail"))),
        Err(e) => {
            let headers = e.metadata();
            let middleware_header = headers.get("x-middleware");
            let value = middleware_header.map(|v| v.to_str().unwrap()).unwrap_or("");

            if value != "expected value" {
                let msg = format!(
                    "On failing call: Expected to receive header 'x-middleware: expected value', \
                     but instead got: '{}'",
                    value
                );
                return Err(Box::new(Status::internal(msg)));
            }
        }
    }

    // This call should succeed
    descriptor.cmd = b"success".to_vec();
    let resp = client.get_flight_info(Request::new(descriptor)).await?;

    let headers = resp.metadata();
    let middleware_header = headers.get("x-middleware");
    let value = middleware_header.map(|v| v.to_str().unwrap()).unwrap_or("");

    if value != "expected value" {
        let msg = format!(
            "On success call: Expected to receive header 'x-middleware: expected value', \
            but instead got: '{}'",
            value
        );
        return Err(Box::new(Status::internal(msg)));
    }

    Ok(())
}

fn middleware_interceptor(mut req: Request<()>) -> Result<Request<()>, Status> {
    let metadata = req.metadata_mut();
    metadata.insert("x-middleware", "expected value".parse().unwrap());
    Ok(req)
}

async fn auth_basic_proto_scenario(host: &str, port: &str) -> Result {
    let url = format!("http://{}:{}", host, port);
    let mut client = FlightServiceClient::connect(url).await?;

    let action = arrow_flight::Action::default();

    let resp = client.do_action(Request::new(action.clone())).await;
    // This client is unauthenticated and should fail.
    match resp {
        Err(e) => {
            if e.code() != tonic::Code::Unauthenticated {
                return Err(Box::new(Status::internal(format!(
                    "Expected UNAUTHENTICATED but got {:?}",
                    e
                ))));
            }
        }
        Ok(other) => {
            return Err(Box::new(Status::internal(format!(
                "Expected UNAUTHENTICATED but got {:?}",
                other
            ))));
        }
    }

    let token = authenticate(&mut client, AUTH_USERNAME, AUTH_PASSWORD)
        .await
        .expect("must respond successfully from handshake");

    let mut request = Request::new(action);
    let metadata = request.metadata_mut();
    metadata.insert_bin(
        "auth-token-bin",
        MetadataValue::from_bytes(token.as_bytes()),
    );

    let resp = client.do_action(request).await?;
    let mut resp = resp.into_inner();

    let r = resp
        .next()
        .await
        .expect("No response received")
        .expect("Invalid response received");

    let body = String::from_utf8(r.body).unwrap();
    assert_eq!(body, AUTH_USERNAME);

    Ok(())
}

async fn authenticate(
    client: &mut Client,
    username: &str,
    password: &str,
) -> Result<String> {
    let auth = BasicAuth {
        username: username.into(),
        password: password.into(),
    };
    let mut payload = vec![];
    auth.encode(&mut payload)?;

    let req = stream::once(async {
        HandshakeRequest {
            payload,
            ..HandshakeRequest::default()
        }
    });

    let rx = client.handshake(Request::new(req)).await?;
    let mut rx = rx.into_inner();

    let r = rx.next().await.expect("must respond from handshake")?;
    assert!(rx.next().await.is_none(), "must not respond a second time");

    Ok(String::from_utf8(r.payload).unwrap())
}

async fn integration_test_scenario(host: &str, port: &str, path: &str) -> Result {
    let url = format!("http://{}:{}", host, port);

    let client = FlightServiceClient::connect(url).await?;

    let ArrowFile {
        schema, batches, ..
    } = read_json_file(path)?;

    let schema = Arc::new(schema);

    let mut descriptor = FlightDescriptor::default();
    descriptor.set_type(DescriptorType::Path);
    descriptor.path = vec![path.to_string()];

    upload_data(
        client.clone(),
        schema.clone(),
        descriptor.clone(),
        batches.clone(),
    )
    .await?;
    verify_data(client, descriptor, schema, &batches).await?;

    Ok(())
}

async fn upload_data(
    mut client: Client,
    schema: SchemaRef,
    descriptor: FlightDescriptor,
    original_data: Vec<RecordBatch>,
) -> Result {
    let (mut upload_tx, upload_rx) = mpsc::channel(10);

    let options = arrow::ipc::writer::IpcWriteOptions::default();
    let mut schema_flight_data =
        arrow_flight::utils::flight_data_from_arrow_schema(&schema, &options);
    schema_flight_data.flight_descriptor = Some(descriptor.clone());
    upload_tx.send(schema_flight_data).await?;

    let mut original_data_iter = original_data.iter().enumerate();

    if let Some((counter, first_batch)) = original_data_iter.next() {
        let metadata = counter.to_string().into_bytes();
        // Preload the first batch into the channel before starting the request
        send_batch(&mut upload_tx, &metadata, first_batch, &options).await?;

        let outer = client.do_put(Request::new(upload_rx)).await?;
        let mut inner = outer.into_inner();

        let r = inner
            .next()
            .await
            .expect("No response received")
            .expect("Invalid response received");
        assert_eq!(metadata, r.app_metadata);

        // Stream the rest of the batches
        for (counter, batch) in original_data_iter {
            let metadata = counter.to_string().into_bytes();
            send_batch(&mut upload_tx, &metadata, batch, &options).await?;

            let r = inner
                .next()
                .await
                .expect("No response received")
                .expect("Invalid response received");
            assert_eq!(metadata, r.app_metadata);
        }
    } else {
        drop(upload_tx);
        client.do_put(Request::new(upload_rx)).await?;
    }

    Ok(())
}

async fn send_batch(
    upload_tx: &mut mpsc::Sender<FlightData>,
    metadata: &[u8],
    batch: &RecordBatch,
    options: &writer::IpcWriteOptions,
) -> Result {
    let (dictionary_flight_data, mut batch_flight_data) =
        arrow_flight::utils::flight_data_from_arrow_batch(batch, &options);

    upload_tx
        .send_all(&mut stream::iter(dictionary_flight_data).map(Ok))
        .await?;

    // Only the record batch's FlightData gets app_metadata
    batch_flight_data.app_metadata = metadata.to_vec();
    upload_tx.send(batch_flight_data).await?;
    Ok(())
}

async fn verify_data(
    mut client: Client,
    descriptor: FlightDescriptor,
    expected_schema: SchemaRef,
    expected_data: &[RecordBatch],
) -> Result {
    let resp = client.get_flight_info(Request::new(descriptor)).await?;
    let info = resp.into_inner();

    assert!(
        !info.endpoint.is_empty(),
        "No endpoints returned from Flight server",
    );
    for endpoint in info.endpoint {
        let ticket = endpoint
            .ticket
            .expect("No ticket returned from Flight server");

        assert!(
            !endpoint.location.is_empty(),
            "No locations returned from Flight server",
        );
        for location in endpoint.location {
            consume_flight_location(
                location,
                ticket.clone(),
                &expected_data,
                expected_schema.clone(),
            )
            .await?;
        }
    }

    Ok(())
}

async fn consume_flight_location(
    location: Location,
    ticket: Ticket,
    expected_data: &[RecordBatch],
    schema: SchemaRef,
) -> Result {
    let mut location = location;
    // The other Flight implementations use the `grpc+tcp` scheme, but the Rust http libs
    // don't recognize this as valid.
    location.uri = location.uri.replace("grpc+tcp://", "grpc://");

    let mut client = FlightServiceClient::connect(location.uri).await?;
    let resp = client.do_get(ticket).await?;
    let mut resp = resp.into_inner();

    // We already have the schema from the FlightInfo, but the server sends it again as the
    // first FlightData. Ignore this one.
    let _schema_again = resp.next().await.unwrap();

    let mut dictionaries_by_field = vec![None; schema.fields().len()];

    for (counter, expected_batch) in expected_data.iter().enumerate() {
        let data = receive_batch_flight_data(
            &mut resp,
            schema.clone(),
            &mut dictionaries_by_field,
        )
        .await
        .unwrap_or_else(|| {
            panic!(
                "Got fewer batches than expected, received so far: {} expected: {}",
                counter,
                expected_data.len(),
            )
        });

        let metadata = counter.to_string().into_bytes();
        assert_eq!(metadata, data.app_metadata);

        let actual_batch =
            flight_data_to_arrow_batch(&data, schema.clone(), &dictionaries_by_field)
                .expect("Unable to convert flight data to Arrow batch");

        assert_eq!(expected_batch.schema(), actual_batch.schema());
        assert_eq!(expected_batch.num_columns(), actual_batch.num_columns());
        assert_eq!(expected_batch.num_rows(), actual_batch.num_rows());
        let schema = expected_batch.schema();
        for i in 0..expected_batch.num_columns() {
            let field = schema.field(i);
            let field_name = field.name();

            let expected_data = expected_batch.column(i).data();
            let actual_data = actual_batch.column(i).data();

            assert_eq!(expected_data, actual_data, "Data for field {}", field_name);
        }
    }

    assert!(
        resp.next().await.is_none(),
        "Got more batches than the expected: {}",
        expected_data.len(),
    );

    Ok(())
}

async fn receive_batch_flight_data(
    resp: &mut Streaming<FlightData>,
    schema: SchemaRef,
    dictionaries_by_field: &mut [Option<ArrayRef>],
) -> Option<FlightData> {
    let mut data = resp.next().await?.ok()?;
    let mut message = arrow::ipc::root_as_message(&data.data_header[..])
        .expect("Error parsing first message");

    while message.header_type() == ipc::MessageHeader::DictionaryBatch {
        reader::read_dictionary(
            &data.data_body,
            message
                .header_as_dictionary_batch()
                .expect("Error parsing dictionary"),
            &schema,
            dictionaries_by_field,
        )
        .expect("Error reading dictionary");

        data = resp.next().await?.ok()?;
        message = arrow::ipc::root_as_message(&data.data_header[..])
            .expect("Error parsing message");
    }

    Some(data)
}
