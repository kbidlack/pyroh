use iroh::endpoint::{ReadError, RecvStream, SendStream};
use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::future_into_py;
use std::sync::Arc;
use tokio::sync::Mutex;

#[pyclass]
pub struct IrohSendStream {
    pub inner: Arc<Mutex<SendStream>>,
    pub id: u64,
}

impl IrohSendStream {
    pub fn new(stream: SendStream) -> Self {
        let id = stream.id().index();
        Self {
            inner: Arc::new(Mutex::new(stream)),
            id,
        }
    }
}

#[pymethods]
impl IrohSendStream {
    fn write<'py>(&self, py: Python<'py>, data: Vec<u8>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        future_into_py(py, async move {
            inner
                .lock()
                .await
                .write_all(&data)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))
        })
    }

    fn finish<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        future_into_py(py, async move {
            let _ = inner.lock().await.finish();
            Ok(())
        })
    }
}

#[pyclass]
pub struct IrohRecvStream {
    pub inner: Arc<Mutex<RecvStream>>,
    pub id: u64,
}

impl IrohRecvStream {
    pub fn new(stream: RecvStream) -> Self {
        let id = stream.id().index();
        Self {
            inner: Arc::new(Mutex::new(stream)),
            id,
        }
    }
}

#[pymethods]
impl IrohRecvStream {
    fn read<'py>(&self, py: Python<'py>, max_bytes: usize) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        future_into_py(py, async move {
            let mut buf = vec![0u8; max_bytes];
            let n = match inner.lock().await.read(&mut buf).await {
                Ok(n) => n,
                Err(ReadError::ClosedStream | ReadError::ConnectionLost(_)) => None,
                Err(e) => return Err(PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string())),
            };
            buf.truncate(n.unwrap_or(0));
            Ok(buf)
        })
    }
}
