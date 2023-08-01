use datafusion::prelude::SessionContext;
use jni::{JNIEnv, objects::JClass, sys::jlong};

#[no_mangle]
pub extern "system" fn Java_com_github_akoshchiy_datafusion_JniSessionContext_createSessionContext(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    let context = SessionContext::new();
    Box::into_raw(Box::new(context)) as jlong
}

#[no_mangle]
pub extern "system" fn Java_com_github_akoshchiy_datafusion_JniSessionContext_destroySessionContext(
    _env: JNIEnv,
    _class: JClass,
    pointer: jlong,
) {
    let _ = unsafe { Box::from_raw(pointer as *mut SessionContext) };
}
