use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use aos_zenoh::security::{ClientTlsProfile, RouterTlsProfile};

pub struct Certificates {
    directory: PathBuf,
}

impl Certificates {
    pub fn new() -> Self {
        let directory = std::env::temp_dir().join(format!(
            "aos-security-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir(&directory).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&directory, fs::Permissions::from_mode(0o700)).unwrap();
        }
        let result = Self { directory };
        for root in ["ca", "other_ca"] {
            result.openssl(&[
                "req",
                "-x509",
                "-newkey",
                "ec",
                "-pkeyopt",
                "ec_paramgen_curve:prime256v1",
                "-nodes",
                "-sha256",
                "-days",
                "2",
                "-subj",
                &format!("/CN={root}"),
                "-keyout",
                &format!("{root}.key"),
                "-out",
                &format!("{root}.pem"),
                "-addext",
                "basicConstraints=critical,CA:TRUE",
                "-addext",
                "keyUsage=critical,keyCertSign,cRLSign",
            ]);
        }
        result
    }

    fn openssl(&self, args: &[&str]) {
        let output = Command::new("openssl")
            .args(args)
            .current_dir(&self.directory)
            .output()
            .expect("OpenSSL is required for native TLS tests");
        assert!(
            output.status.success(),
            "OpenSSL fixture failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    pub fn issue(&self, name: &str, cn: &str, root: &str, san: &str, expired: bool) {
        self.openssl(&[
            "req",
            "-new",
            "-newkey",
            "ec",
            "-pkeyopt",
            "ec_paramgen_curve:prime256v1",
            "-nodes",
            "-subj",
            &format!("/CN={cn}"),
            "-keyout",
            &format!("{name}.key"),
            "-out",
            &format!("{name}.csr"),
        ]);
        let extension = format!("basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature,keyAgreement\nextendedKeyUsage=serverAuth,clientAuth\nsubjectAltName={san}\n");
        fs::write(self.directory.join(format!("{name}.ext")), &extension).unwrap();
        if expired {
            fs::write(self.directory.join("index"), "").unwrap();
            fs::write(self.directory.join("serial"), "1000\n").unwrap();
            fs::write(self.directory.join("ca.conf"), format!("[ca]\ndefault_ca=default\n[default]\ndatabase=index\nserial=serial\nnew_certs_dir=.\ncertificate={root}.pem\nprivate_key={root}.key\ndefault_md=sha256\npolicy=policy\nx509_extensions=leaf\n[policy]\ncommonName=supplied\n[leaf]\n{extension}")).unwrap();
            self.openssl(&[
                "ca",
                "-batch",
                "-notext",
                "-config",
                "ca.conf",
                "-in",
                &format!("{name}.csr"),
                "-out",
                &format!("{name}.pem"),
                "-startdate",
                "20200101000000Z",
                "-enddate",
                "20200102000000Z",
            ]);
        } else {
            self.openssl(&[
                "x509",
                "-req",
                "-sha256",
                "-days",
                "2",
                "-in",
                &format!("{name}.csr"),
                "-CA",
                &format!("{root}.pem"),
                "-CAkey",
                &format!("{root}.key"),
                "-set_serial",
                &SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
                    .to_string(),
                "-extfile",
                &format!("{name}.ext"),
                "-out",
                &format!("{name}.pem"),
            ]);
        }
    }

    pub fn path(&self, name: &str) -> String {
        self.directory.join(name).to_str().unwrap().to_owned()
    }
    pub fn directory(&self) -> &Path {
        &self.directory
    }

    pub fn client(&self, endpoint: &str, name: &str, domain: u32) -> ClientTlsProfile {
        ClientTlsProfile {
            schema_version: 1,
            domain_id: domain,
            router_endpoints: vec![endpoint.into()],
            root_ca_certificate: self.path("ca.pem"),
            certificate: self.path(&format!("{name}.pem")),
            private_key: self.path(&format!("{name}.key")),
            connect_timeout_ms: 1500,
            peer: None,
        }
    }
    pub fn router(&self, endpoint: &str, name: &str) -> RouterTlsProfile {
        RouterTlsProfile {
            local_peers: false,
            schema_version: 1,
            listen_endpoints: vec![endpoint.into()],
            root_ca_certificate: self.path("ca.pem"),
            certificate: self.path(&format!("{name}.pem")),
            private_key: self.path(&format!("{name}.key")),
            federation: None,
        }
    }
}

impl Drop for Certificates {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.directory);
    }
}

#[derive(Clone, Default)]
pub struct AclLogs(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for AclLogs {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        let mut buffer = self.0.lock().unwrap();
        assert!(
            buffer.len() + data.len() < 8 * 1024 * 1024,
            "ACL evidence exceeded its bound"
        );
        buffer.extend_from_slice(data);
        Ok(data.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl AclLogs {
    pub fn install() -> Self {
        Self::install_filter("off,zenoh::net::routing::interceptor=trace", false)
    }

    // The shared support module is also compiled by ACL-only test binaries.
    #[allow(dead_code)]
    pub fn install_with_routing() -> Self {
        Self::install_filter(
            "off,zenoh::net::routing::interceptor=trace,zenoh::net::routing::dispatcher::pubsub=trace,zenoh::net::routing::hat::client::pubsub=debug,zenoh::net::routing::hat::client::interests=debug",
            true,
        )
    }

    fn install_filter(filter: &str, thread_names: bool) -> Self {
        let logs = Self::default();
        let writer = logs.clone();
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_ansi(false)
            .with_thread_ids(true)
            .with_thread_names(thread_names)
            .without_time()
            .with_writer(move || writer.clone())
            .init();
        logs
    }
    pub fn text(&self) -> String {
        String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
    }
    pub fn contains_denial(&self, key: &str) -> bool {
        self.text()
            .lines()
            .any(|line| line.contains("unauthorized") && line.contains(key))
    }

    pub fn message_denials(&self, key: &str, operation: &str) -> Vec<String> {
        // Permission-cache construction emits allow/deny decisions too. The
        // interceptor chain's following marker is emitted only when an actual
        // message was dropped. Both functions execute synchronously on the
        // same thread; track every event so unrelated events cannot be paired.
        let mut previous = std::collections::BTreeMap::<String, String>::new();
        let mut evidence = Vec::new();
        for line in self.text().lines() {
            let Some((_, suffix)) = line.split_once("ThreadId(") else {
                continue;
            };
            let Some((thread, _)) = suffix.split_once(')') else {
                continue;
            };
            if line.contains("zenoh::net::routing::interceptor: Msg intercepted!") {
                if let Some(decision) = previous.get(thread) {
                    if decision.contains("access_control:")
                        && decision.contains("unauthorized")
                        && decision.contains(operation)
                        && decision
                            .rsplit_once(" on ")
                            .is_some_and(|(_, target)| target == key)
                    {
                        evidence.push(format!("{decision}\n{line}"));
                    }
                }
            }
            previous.insert(thread.to_owned(), line.to_owned());
        }
        evidence
    }
}
