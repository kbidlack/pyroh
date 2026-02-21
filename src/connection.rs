use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::future_into_py;
use std::sync::Arc;

use crate::streams::{IrohRecvStream, IrohSendStream};

#[pyclass]
pub struct IrohConnection {
    pub inner: Arc<iroh::endpoint::Connection>,
}

#[pymethods]
impl IrohConnection {
    fn open_bi<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        future_into_py(py, async move {
            let (send, recv) = inner
                .open_bi()
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))?;
            Ok((IrohSendStream::new(send), IrohRecvStream::new(recv)))
        })
    }

    fn accept_bi<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        future_into_py(py, async move {
            let (send, recv) = inner
                .accept_bi()
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))?;
            Ok((IrohSendStream::new(send), IrohRecvStream::new(recv)))
        })
    }

    fn open_uni<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        future_into_py(py, async move {
            let send = inner
                .open_uni()
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))?;
            Ok(IrohSendStream::new(send))
        })
    }

    fn accept_uni<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        future_into_py(py, async move {
            let recv = inner
                .accept_uni()
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))?;
            Ok(IrohRecvStream::new(recv))
        })
    }

    #[getter]
    fn alpn(&self) -> Vec<u8> {
        self.inner.alpn().to_vec()
    }

    fn close<'py>(
        &self,
        py: Python<'py>,
        error_code: u32,
        reason: Vec<u8>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        future_into_py(py, async move {
            inner.close(error_code.into(), &reason);
            inner.closed().await;
            Ok(())
        })
    }
}
