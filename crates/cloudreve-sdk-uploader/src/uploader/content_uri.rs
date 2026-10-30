//! Android `content://` range reader.
//!
//! Uses the Activity's `ContentResolver` through `ndk-context`, detaches the
//! `ParcelFileDescriptor`, and turns the raw fd into a seekable tokio file.

use std::os::fd::FromRawFd;

use anyhow::{Context, Result, anyhow, bail};
use jni::objects::{JObject, JValue};
use jni::JavaVM;
use tokio::io::{AsyncReadExt, AsyncSeekExt, BufReader, SeekFrom};

use super::source::SourceReader;

pub(crate) async fn open_range(
    uri: &str,
    offset: u64,
    len: u64,
) -> Result<SourceReader> {
    // Keep every JNI local reference and the AttachGuard inside this block:
    // none of them are `Send`, and the chunk opener future must be `Send`.
    let file = {
        let context = ndk_context::android_context();
        let vm = unsafe { JavaVM::from_raw(context.vm().cast()) }
            .map_err(|error| anyhow!("failed to get Android JavaVM: {error}"))?;
        let mut env = vm
            .attach_current_thread()
            .map_err(|error| anyhow!("failed to attach JNI thread: {error}"))?;

        let context = unsafe { JObject::from_raw(context.context().cast()) };
        let resolver = env
            .call_method(
                &context,
                "getContentResolver",
                "()Landroid/content/ContentResolver;",
                &[],
            )
            .context("getContentResolver")?
            .l()
            .context("ContentResolver is null")?;

        let uri_string = env.new_string(uri).context("create content URI string")?;
        let uri_object = env
            .call_static_method(
                "android/net/Uri",
                "parse",
                "(Ljava/lang/String;)Landroid/net/Uri;",
                &[JValue::Object(&uri_string)],
            )
            .context("Uri.parse")?
            .l()
            .context("Uri.parse returned null")?;

        let mode = env.new_string("r").context("create file mode")?;
        let descriptor = env
            .call_method(
                &resolver,
                "openFileDescriptor",
                "(Landroid/net/Uri;Ljava/lang/String;)Landroid/os/ParcelFileDescriptor;",
                &[JValue::Object(&uri_object), JValue::Object(&mode)],
            )
            .context("ContentResolver.openFileDescriptor")?
            .l()
            .context("content provider returned a null descriptor")?;

        let raw_fd = env
            .call_method(&descriptor, "detachFd", "()I", &[])
            .context("ParcelFileDescriptor.detachFd")?
            .i()
            .context("detachFd returned a non-int")?;
        if raw_fd < 0 {
            bail!("content provider returned invalid fd {raw_fd}");
        }
        // SAFETY: detachFd transfers ownership of the descriptor to us.
        unsafe { std::fs::File::from_raw_fd(raw_fd) }
    };

    let mut reader = BufReader::with_capacity(64 * 1024, tokio::fs::File::from_std(file));
    reader
        .seek(SeekFrom::Start(offset))
        .await
        .context("content:// descriptor is not seekable")?;
    Ok(Box::new(reader.take(len)) as SourceReader)
}
