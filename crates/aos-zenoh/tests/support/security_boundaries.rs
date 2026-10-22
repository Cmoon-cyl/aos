//! Additional temporary certificate shapes for native boundary tests.
use std::{
    fs,
    process::Command,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use super::support::Certificates;

fn openssl(certificates: &Certificates, args: &[&str]) -> String {
    let output = Command::new("openssl")
        .args(args)
        .current_dir(certificates.directory())
        .output()
        .expect("OpenSSL is required for native TLS tests");
    assert!(
        output.status.success(),
        "OpenSSL fixture failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap()
}

#[cfg(unix)]
fn utc(time: SystemTime) -> String {
    let seconds = time.duration_since(UNIX_EPOCH).unwrap().as_secs() as libc::time_t;
    let mut result = std::mem::MaybeUninit::<libc::tm>::uninit();
    // gmtime_r writes the supplied tm and retains neither pointer.
    assert!(!unsafe { libc::gmtime_r(&seconds, result.as_mut_ptr()) }.is_null());
    let value = unsafe { result.assume_init() };
    format!(
        "{:04}{:02}{:02}{:02}{:02}{:02}Z",
        value.tm_year + 1900,
        value.tm_mon + 1,
        value.tm_mday,
        value.tm_hour,
        value.tm_min,
        value.tm_sec
    )
}

#[cfg(unix)]
pub fn issue_short(certificates: &Certificates, name: &str, cn: &str) -> SystemTime {
    let expires = SystemTime::now() + Duration::from_secs(8);
    issue(certificates, name, &format!("/CN={cn}"), expires);
    expires
}

#[cfg(unix)]
pub fn issue_without_cn(certificates: &Certificates, name: &str) {
    issue(
        certificates,
        name,
        "/O=AOS boundary fixture",
        SystemTime::now() + Duration::from_secs(3600),
    );
    let subject = openssl(
        certificates,
        &[
            "x509",
            "-in",
            &format!("{name}.pem"),
            "-noout",
            "-subject",
            "-nameopt",
            "RFC2253",
        ],
    );
    assert!(
        !subject.contains("CN="),
        "missing-CN fixture accidentally has a CN"
    );
}

#[cfg(unix)]
fn issue(certificates: &Certificates, name: &str, subject: &str, expires: SystemTime) {
    openssl(
        certificates,
        &[
            "req",
            "-new",
            "-newkey",
            "ec",
            "-pkeyopt",
            "ec_paramgen_curve:prime256v1",
            "-nodes",
            "-subj",
            subject,
            "-keyout",
            &format!("{name}.key"),
            "-out",
            &format!("{name}.csr"),
        ],
    );
    fs::write(certificates.directory().join(format!("{name}.index")), "").unwrap();
    fs::write(
        certificates.directory().join(format!("{name}.serial")),
        "1000\n",
    )
    .unwrap();
    let config = format!("[ca]\ndefault_ca=default\n[default]\ndatabase={name}.index\nserial={name}.serial\nnew_certs_dir=.\ncertificate=ca.pem\nprivate_key=ca.key\ndefault_md=sha256\npolicy=policy\nx509_extensions=leaf\n[policy]\ncommonName=optional\norganizationName=optional\n[leaf]\nbasicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature,keyAgreement\nextendedKeyUsage=serverAuth,clientAuth\nsubjectAltName=DNS:localhost,IP:127.0.0.1\n");
    fs::write(
        certificates.directory().join(format!("{name}.conf")),
        config,
    )
    .unwrap();
    openssl(
        certificates,
        &[
            "ca",
            "-batch",
            "-notext",
            "-config",
            &format!("{name}.conf"),
            "-in",
            &format!("{name}.csr"),
            "-out",
            &format!("{name}.pem"),
            "-startdate",
            &utc(SystemTime::now() - Duration::from_secs(30)),
            "-enddate",
            &utc(expires),
        ],
    );
    let metadata = openssl(
        certificates,
        &[
            "x509",
            "-in",
            &format!("{name}.pem"),
            "-noout",
            "-subject",
            "-dates",
            "-fingerprint",
            "-sha256",
        ],
    );
    println!("certificate fixture {name}:\n{metadata}");
}
