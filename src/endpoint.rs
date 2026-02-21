use crate::connection::IrohConnection;
use iroh::KeyParsingError;
use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::future_into_py;
use std::sync::Arc;

#[pyclass]
pub struct IrohEndpoint {
    inner: iroh::Endpoint,
}

#[pymethods]
impl IrohEndpoint {
    /// bind a new endpoint that accepts connections using any of the given ALPNs
    #[staticmethod]
    fn bind(py: Python<'_>, alpns: Vec<Vec<u8>>) -> PyResult<Bound<'_, PyAny>> {
        future_into_py(py, async move {
            let ep = iroh::Endpoint::builder()
                .alpns(alpns)
                .bind()
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))?;
            Ok(IrohEndpoint { inner: ep })
        })
    }

    /// wait until the endpoint has contacted a relay
    /// does not have a built in timeout
    fn wait_online<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let ep = self.inner.clone();
        future_into_py(py, async move {
            ep.online().await;
            Ok(())
        })
    }

    /// endpoint address (public key) string
    #[getter]
    fn addr(&self) -> String {
        self.inner.addr().id.to_string()
    }

    /// accept the next incoming connection with any registered ALPN
    fn accept<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let ep = self.inner.clone();
        future_into_py(py, async move {
            let conn = ep
                .accept()
                .await
                .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyIOError, _>("endpoint closed"))?
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))?;
            Ok(IrohConnection {
                inner: Arc::new(conn),
            })
        })
    }

    /// connect to a remote endpoint with the given ALPN
    fn connect<'py>(
        &self,
        py: Python<'py>,
        addr: String,
        alpn: Vec<u8>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ep = self.inner.clone();
        future_into_py(py, async move {
            let node_addr: iroh::EndpointId = addr.parse().map_err(|e: KeyParsingError| {
                PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string())
            })?;
            let conn = ep
                .connect(node_addr, &alpn)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))?;
            Ok(IrohConnection {
                inner: Arc::new(conn),
            })
        })
    }

    fn set_alpns(&self, alpns: Vec<Vec<u8>>) {
        self.inner.set_alpns(alpns);
    }

    fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let ep = self.inner.clone();
        future_into_py(py, async move {
            ep.close().await;
            Ok(())
        })
    }
}
