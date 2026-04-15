use std::{fs, path::Path};

use rcgen::CertifiedKey;

fn main() {
    let CertifiedKey { cert, signing_key } =
        rcgen::generate_simple_self_signed(["libbft.network".into()]).unwrap();
    fs::write(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("src/server.cert.der"),
        cert.der(),
    )
    .unwrap();
    fs::write(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("src/server.key.der"),
        signing_key.serialized_der(),
    )
    .unwrap();
}
