#[link(wasm_import_module = "the-wasm-import-module")]
extern "C" {
    // imports the name `foo` from `the-wasm-import-module`
    fn foo();

    // functions can have integer/float arguments/return values
    fn translate(a: i32) -> f32;

    // you can also explicitly specify the name to import, this imports `bar`
    // instead of `baz` from `the-wasm-import-module`.
    #[link_name = "bar"]
    fn baz();
}