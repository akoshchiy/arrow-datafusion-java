use std::{future::Future, sync::mpsc};

use jni::JNIEnv;
use tokio::runtime::Runtime;

pub fn spawn<F>(rt: &Runtime, f: F)
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let (tx, rx) = mpsc::channel();
    rt.spawn(async move {
        tx.send(()).unwrap();
        f.await;
    });
    rx.recv().unwrap();
}

pub fn throw_ex(env: &mut JNIEnv, msg: String) {
    env.throw_new("com/github/akoshchiy/datafusion/DatafusionException", msg)
        .unwrap();
}
