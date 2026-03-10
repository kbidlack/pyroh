use pyo3::prelude::*;

mod connection;
mod endpoint;
mod secret_key;
mod streams;

#[pymodule]
fn _iroh(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<endpoint::IrohEndpoint>()?;
    m.add_class::<connection::IrohConnection>()?;
    m.add_class::<secret_key::IrohSecretKey>()?;
    m.add_class::<streams::IrohSendStream>()?;
    m.add_class::<streams::IrohRecvStream>()?;
    Ok(())
}
