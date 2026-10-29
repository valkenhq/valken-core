/*
Sourced from godot-tokio (https://github.com/2-3-5-41/godot_tokio) under MIT license.

MIT License

Copyright (c) 2025 2-3-5-41

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

use std::{future::Future, rc::Rc};

use godot::{classes::Engine, prelude::*};
use tokio::runtime::{self, Runtime};

/// Engine singleton that owns the process-wide Tokio runtime.
///
/// # Threading
///
/// Every associated function here goes through `Engine::singleton()`, and
/// this crate does not enable godot-rust's `experimental-threads` feature, so
/// **they must be called from the Godot main thread**.  Background tasks that
/// need to spawn more work should capture a [`tokio::runtime::Handle`] from
/// [`AsyncRuntime::handle`] on the main thread first; the handle is `Send`.
#[derive(GodotClass)]
#[class(base=Object)]
pub struct AsyncRuntime {
    base: Base<Object>,
    runtime: Rc<Runtime>,
}

#[godot_api]
impl IObject for AsyncRuntime {
    fn init(base: Base<Object>) -> Self {
        let runtime = runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();

        Self {
            base,
            runtime: Rc::new(runtime),
        }
    }
}

#[godot_api]
impl AsyncRuntime {
    pub const SINGLETON: &'static str = "Tokio";

    /// This function has no real use for the user, only to make it easier
    /// for this crate to access the singleton object.
    fn singleton() -> Option<Gd<AsyncRuntime>> {
        Engine::singleton()
            .get_singleton(Self::SINGLETON)
            .map(|singleton| singleton.cast::<Self>())
    }

    /// **Can Panic**
    ///
    /// Gets the active runtime under the [`AsyncRuntime`] singleton. This can panic if the singleton is unreachable,
    /// or has no ability to be registered by the engine.
    pub fn runtime() -> Rc<Runtime> {
        match Self::singleton() {
            Some(singleton) => {
                let bind = singleton.bind();
                Rc::clone(&bind.runtime)
            }
            None => {
                // Assuming the user forgot to register the singleton, let's do it for them as a fail safe.
                Engine::singleton()
                    .register_singleton(AsyncRuntime::SINGLETON, &AsyncRuntime::new_alloc());

                // We can panic here because something fundementally wrong has happend.
                let singleton = Self::singleton()
                    .expect("Engine was not able to register, or get `AsyncRuntime` singleton!");

                let bind = singleton.bind();
                Rc::clone(&bind.runtime)
            }
        }
    }

    /// A `Send` handle to the runtime, for passing into background code.
    /// Main thread only (see the type-level docs).
    pub fn handle() -> tokio::runtime::Handle {
        Self::runtime().handle().clone()
    }

    /// A wrapper function for the [`tokio::spawn`] function.
    /// Main thread only (see the type-level docs).
    pub fn spawn<F>(future: F) -> tokio::task::JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        Self::runtime().spawn(future)
    }
}
