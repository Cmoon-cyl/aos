//! A real TCP relay with an adjustable minimum one-way delivery delay.
//! Each direction preserves byte order; no middleware callback is delayed.

use std::io::{Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

pub struct DelayProxy {
    pub endpoint: String,
    delay_ms: Arc<AtomicU64>,
    stopped: Arc<AtomicBool>,
    sockets: Arc<Mutex<Vec<TcpStream>>>,
    worker: Option<JoinHandle<()>>,
}

impl DelayProxy {
    pub fn start(upstream: &str) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("tcp/{}", listener.local_addr().unwrap());
        listener.set_nonblocking(true).unwrap();
        let upstream = upstream.strip_prefix("tcp/").unwrap().to_owned();
        let delay_ms = Arc::new(AtomicU64::new(0));
        let stopped = Arc::new(AtomicBool::new(false));
        let sockets = Arc::new(Mutex::new(Vec::new()));
        let worker = {
            let delay_ms = delay_ms.clone();
            let stopped = stopped.clone();
            let sockets = sockets.clone();
            thread::spawn(move || {
                let mut relays = Vec::new();
                while !stopped.load(Ordering::Acquire) {
                    let downstream = match listener.accept() {
                        Ok((socket, _)) => socket,
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            thread::sleep(Duration::from_millis(5));
                            continue;
                        }
                        Err(error) => panic!("delay proxy accept: {error}"),
                    };
                    let upstream = TcpStream::connect(&upstream).unwrap();
                    for socket in [&downstream, &upstream] {
                        socket.set_nodelay(true).unwrap();
                        socket
                            .set_read_timeout(Some(Duration::from_millis(100)))
                            .unwrap();
                        socket
                            .set_write_timeout(Some(Duration::from_secs(1)))
                            .unwrap();
                        sockets.lock().unwrap().push(socket.try_clone().unwrap());
                    }
                    for (reader, writer) in [
                        (
                            downstream.try_clone().unwrap(),
                            upstream.try_clone().unwrap(),
                        ),
                        (upstream, downstream),
                    ] {
                        let delay_ms = delay_ms.clone();
                        let stopped = stopped.clone();
                        relays.push(thread::spawn(move || {
                            relay(reader, writer, &delay_ms, &stopped);
                        }));
                    }
                }
                for relay in relays {
                    relay.join().unwrap();
                }
            })
        };
        Self {
            endpoint,
            delay_ms,
            stopped,
            sockets,
            worker: Some(worker),
        }
    }

    /// Arm after the handshake so the test measures discovery latency.
    pub fn set_delay(&self, delay: Duration) {
        self.delay_ms
            .store(delay.as_millis().try_into().unwrap(), Ordering::Release);
    }
}

fn relay(mut reader: TcpStream, mut writer: TcpStream, delay_ms: &AtomicU64, stopped: &AtomicBool) {
    let mut buffer = [0; 64 * 1024];
    while !stopped.load(Ordering::Acquire) {
        let count = match reader.read(&mut buffer) {
            Ok(0) => break,
            Ok(count) => count,
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock
                        | std::io::ErrorKind::TimedOut
                        | std::io::ErrorKind::Interrupted
                ) =>
            {
                continue;
            }
            Err(_) => break,
        };
        thread::sleep(Duration::from_millis(delay_ms.load(Ordering::Acquire)));
        if stopped.load(Ordering::Acquire) || writer.write_all(&buffer[..count]).is_err() {
            break;
        }
    }
}

impl Drop for DelayProxy {
    fn drop(&mut self) {
        self.stopped.store(true, Ordering::Release);
        for socket in self.sockets.lock().unwrap().iter() {
            let _ = socket.shutdown(Shutdown::Both);
        }
        if let Some(worker) = self.worker.take() {
            worker.join().unwrap();
        }
    }
}
