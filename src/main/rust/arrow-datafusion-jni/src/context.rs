use std::sync::Arc;

use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use jni::{
    errors::Error,
    objects::{JClass, JObject, JString, JValue},
    sys::jlong,
    Executor, JNIEnv,
};
use tokio::runtime::Runtime;

use crate::util::spawn;

#[no_mangle]
pub extern "system" fn Java_com_github_akoshchiy_datafusion_JniSessionContext_nativeCreate(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    let config = SessionConfig::new().set_usize("datafusion.execution.batch_size", 8192);
    let context = SessionContext::with_config(config);
    Box::into_raw(Box::new(context)) as jlong
}

#[no_mangle]
pub extern "system" fn Java_com_github_akoshchiy_datafusion_JniSessionContext_nativeDestroy(
    _env: JNIEnv,
    _class: JClass,
    context: jlong,
) {
    let _ = unsafe { Box::from_raw(context as *mut SessionContext) };
}

#[no_mangle]
pub extern "system" fn Java_com_github_akoshchiy_datafusion_JniSessionContext_nativeSql(
    mut env: JNIEnv,
    _class: JClass,
    runtime: jlong,
    ctx: jlong,
    sql: JString,
    callback: JObject,
) {
    let runtime = unsafe { &mut *(runtime as *mut Runtime) };
    let ctx = unsafe { &*(ctx as *mut SessionContext) };
    let sql: String = env.get_string(&sql).unwrap().into();

    let jvm = env.get_java_vm().unwrap();
    let callback = env.new_global_ref(callback).unwrap();

    spawn(runtime, async move {
        let result = ctx.sql(&sql).await;
        let executor = Executor::new(Arc::new(jvm));

        executor
            .with_attached(|env| {
                match result {
                    Ok(v) => {
                        let dataframe = Box::into_raw(Box::new(v)) as jlong;
                        env.call_method(
                            &callback,
                            "accept",
                            "(JLjava/lang/String;)V",
                            &[dataframe.into(), JValue::Object(&JObject::null())],
                        )?;
                    }
                    Err(err) => {
                        let dataframe = -1 as jlong;
                        let err = env.new_string(err.to_string()).unwrap();
                        env.call_method(
                            &callback,
                            "accept",
                            "(JLjava/lang/String;)V",
                            &[dataframe.into(), JValue::Object(&JObject::from(err))],
                        )?;
                    }
                }
                Ok::<(), Error>(())
            })
            .unwrap();
    });
}

#[no_mangle]
pub extern "system" fn Java_com_github_akoshchiy_datafusion_JniSessionContext_nativeRegisterParquet(
    mut env: JNIEnv,
    _class: JClass,
    runtime: jlong,
    ctx: jlong,
    name: JString,
    path: JString,
    callback: JObject,
) {
    let ctx = unsafe { &*(ctx as *mut SessionContext) };
    let runtime = unsafe { &mut *(runtime as *mut Runtime) };

    let name: String = env.get_string(&name).unwrap().into();
    let path: String = env.get_string(&path).unwrap().into();

    let jvm = env.get_java_vm().unwrap();
    let callback = env.new_global_ref(callback).unwrap();

    spawn(runtime, async move {
        let result = ctx
            .register_parquet(&name, &path, ParquetReadOptions::default())
            .await;
        let executor = Executor::new(Arc::new(jvm));
        executor
            .with_attached(|env| {
                match result {
                    Ok(_) => {
                        env.call_method(
                            &callback,
                            "accept",
                            "(Ljava/lang/String;)V",
                            &[JValue::Object(&JObject::null())],
                        )?;
                    }
                    Err(err) => {
                        let err = env.new_string(err.to_string()).unwrap();
                        env.call_method(
                            &callback,
                            "accept",
                            "(Ljava/lang/String;)V",
                            &[JValue::Object(&JObject::from(err))],
                        )?;
                    }
                }
                Ok::<(), Error>(())
            })
            .unwrap();
    });
}
