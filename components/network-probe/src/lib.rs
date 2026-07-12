#[allow(warnings)]
mod bindings;

use std::io::ErrorKind;
use std::net::TcpStream;

use bindings::exports::fluxion::task::processor::{Guest, TaskInput, TaskOutput};

struct Component;

impl Guest for Component {
    fn process(input: TaskInput) -> Result<TaskOutput, String> {
        let addr = String::from_utf8(input.content)
            .map_err(|e| e.to_string())?
            .trim()
            .to_string();

        match TcpStream::connect(&addr) {
            Ok(_) => Ok(TaskOutput {
                content: format!("Connected to {addr}").into_bytes(),
                metadata: vec![("status".to_string(), "connected".to_string())],
            }),
            // Reached the network stack (permission was granted); server just not listening.
            Err(e) if e.kind() == ErrorKind::ConnectionRefused => Ok(TaskOutput {
                content: format!("Reached {addr} (no server — permission granted)").into_bytes(),
                metadata: vec![("status".to_string(), "reached".to_string())],
            }),
            Err(e) => Err(format!("Cannot connect to '{addr}': {e}")),
        }
    }
}

bindings::export!(Component with_types_in bindings);
