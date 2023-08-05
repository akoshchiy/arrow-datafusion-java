use std::time::Duration;

use jni::{objects::JClass, sys::jlong, JNIEnv};
use tokio::runtime::Runtime;

use crate::util::throw_ex;

#[no_mangle]
pub extern "system" fn Java_com_github_akoshchiy_datafusion_JniRuntime_nativeCreate(
    mut env: JNIEnv,
    _class: JClass,
) -> jlong {
    let result = Runtime::new();
    match result {
        Ok(rt) => Box::into_raw(Box::new(rt)) as jlong,
        Err(err) => {
            throw_ex(&mut env, err.to_string());
            -1
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_com_github_akoshchiy_datafusion_JniRuntime_nativeDestroy(
    _env: JNIEnv,
    _class: JClass,
    pointer: jlong,
) {
    let runtime = unsafe { Box::from_raw(pointer as *mut Runtime) };
    runtime.shutdown_timeout(Duration::from_millis(1000));
}
