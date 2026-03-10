use iroh::SecretKey;
use pyo3::prelude::*;

#[pyclass]
pub struct IrohSecretKey {
    inner: SecretKey,
}

#[pymethods]
impl IrohSecretKey {
    /// Generate a new random secret key.
    #[staticmethod]
    fn generate() -> IrohSecretKey {
        IrohSecretKey {
            inner: SecretKey::generate(&mut rand::rng()),
        }
    }

    /// Create a secret key from its 32-byte representation.
    #[staticmethod]
    fn from_bytes(bytes: Vec<u8>) -> PyResult<IrohSecretKey> {
        let key_slice: [u8; 32] = bytes
            .try_into()
            .map_err(|_| pyo3::exceptions::PyValueError::new_err("key must be exactly 32 bytes"))?;
        Ok(IrohSecretKey {
            inner: SecretKey::from_bytes(&key_slice),
        })
    }

    /// The secret key as raw bytes. Store these to persist the key across restarts.
    fn to_bytes(&self) -> Vec<u8> {
        self.inner.to_bytes().to_vec()
    }

    /// The node ID (public key) derived from this secret key, as a hex string.
    #[getter]
    fn node_id(&self) -> String {
        self.inner.public().to_string()
    }

    /// The node ID (public key) derived from this secret key, as 32 raw bytes.
    #[getter]
    fn node_id_bytes(&self) -> Vec<u8> {
        self.inner.public().as_bytes().to_vec()
    }

    fn __repr__(&self) -> String {
        format!("SecretKey(node_id={})", self.inner.public())
    }

    fn __bytes__(&self) -> Vec<u8> {
        self.inner.to_bytes().to_vec()
    }
}

/// Allow other Rust modules to extract the inner `SecretKey`.
impl IrohSecretKey {
    pub fn into_inner(self) -> SecretKey {
        self.inner
    }

    pub fn inner(&self) -> &SecretKey {
        &self.inner
    }
}
