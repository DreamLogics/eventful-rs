eventful_rs::use_shard!(shard = crate::Other);

#[eventful_rs::eventful]
pub struct Local {
    pub value: std::rc::Rc<usize>,
}
impl Local {
    pub fn new() -> Self {
        Self {
            value: std::rc::Rc::new(7),
            events: Default::default(),
        }
    }
}
