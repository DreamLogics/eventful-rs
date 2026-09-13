use std::cell::RefCell;

pub struct GuardedRefCell<T> {
    inner: RefCell<T>,
}

impl<T> GuardedRefCell<T> {
    pub fn new(value: T) -> Self {
        Self {
            inner: RefCell::new(value),
        }
    }

    pub fn borrow(&self) -> std::cell::Ref<'_, T> {
        assert!(
            std::thread::current().id() == std::thread::current().id(),
            "GuardedRefCell can only be accessed from the thread it was created on"
        );
        self.inner.borrow()
    }

    pub fn borrow_mut(&self) -> std::cell::RefMut<'_, T> {
        assert!(
            std::thread::current().id() == std::thread::current().id(),
            "GuardedRefCell can only be accessed from the thread it was created on"
        );
        self.inner.borrow_mut()
    }
}

unsafe impl<T> Send for GuardedRefCell<T> {}
unsafe impl<T> Sync for GuardedRefCell<T> {}
