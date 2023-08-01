use jni::JNIEnv;

pub fn throw_ex(env: &mut JNIEnv, msg: String) {
    env.throw_new("com/github/akoshchiy/datafusion/DatafusionException", msg).unwrap();
}
