build:
	smdk build -p window-helsinki --release release-lto


test_json: build
	smdk test --file test/test.json --raw --wasm-file ../../target/wasm32-unknown-unknown/release-lto/window_helsinki.wasm


unit_test:
	cargo test -p window-example