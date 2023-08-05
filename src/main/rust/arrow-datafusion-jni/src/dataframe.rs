use std::{ptr::addr_of, sync::Arc};

use arrow::{datatypes::Schema, ffi_stream::FFI_ArrowArrayStream};
use datafusion::prelude::DataFrame;
use jni::{
    errors::Error,
    objects::{JClass, JObject, JValue},
    sys::jlong,
    Executor, JNIEnv,
};
use tokio::runtime::Runtime;

use crate::{
    batch::{self, RecordBatchVecReader},
    util::spawn,
};

#[no_mangle]
pub extern "system" fn Java_com_github_akoshchiy_datafusion_JniDataFrame_nativeCollect(
    env: JNIEnv,
    _class: JClass,
    runtime: jlong,
    dataframe: jlong,
    callback: JObject,
) {
    let runtime = unsafe { &*(runtime as *mut Runtime) };
    let dataframe = unsafe { &*(dataframe as *mut DataFrame) };
    let jvm = env.get_java_vm().unwrap();
    let callback = env.new_global_ref(callback).unwrap();
    let dataframe = dataframe.clone();

    spawn(runtime, async move {
        let schema = Arc::new(Schema::from(dataframe.schema()));
        let result = dataframe.collect().await;
        let executor = Executor::new(Arc::new(jvm));

        executor
            .with_attached(|env| {
                match result {
                    Ok(batches) => {
                        let batch = batch::concat(schema.clone(), batches).unwrap();
                        let ffi_stream = FFI_ArrowArrayStream::new(Box::new(
                            RecordBatchVecReader::new(schema, vec![batch]),
                        ));
                        env.call_method(
                            callback,
                            "accept",
                            "(JLjava/lang/String;)V",
                            &[
                                (addr_of!(ffi_stream) as jlong).into(),
                                JValue::Object(&JObject::null()),
                            ],
                        )?;
                    }
                    Err(err) => {
                        let err = env.new_string(err.to_string()).unwrap();
                        env.call_method(
                            callback,
                            "accept",
                            "(JLjava/lang/String;)V",
                            &[(-1 as jlong).into(), JValue::Object(&err)],
                        )?;
                    }
                };
                Ok::<(), Error>(())
            })
            .unwrap();
    });
}
