use std::time::Duration;

use jni::{JNIEnv, objects::JClass, sys::jlong};
use tokio::runtime::Runtime;

use crate::error;

#[no_mangle]
pub extern "system" fn Java_com_github_akoshchiy_datafusion_JniRuntime_createRuntime(
    mut env: JNIEnv,
    _class: JClass,
) -> jlong {
    let result = Runtime::new();
    match result {
        Ok(rt) => {
            println!("rt created!");
            Box::into_raw(Box::new(rt)) as jlong
        },
        Err(err) => {
            error::throw_ex(&mut env, err.to_string());
            -1
        },
    }
}

#[no_mangle]
pub extern "system" fn Java_com_github_akoshchiy_datafusion_JniRuntime_destroyRuntime(
    _env: JNIEnv,
    _class: JClass,
    pointer: jlong,
) {
    let runtime = unsafe { Box::from_raw(pointer as *mut Runtime) };
    runtime.shutdown_timeout(Duration::from_millis(1000));
}
