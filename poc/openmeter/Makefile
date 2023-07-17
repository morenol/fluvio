build:
	smdk build -p poc-openmeter --release release-lto


test_json: build
	smdk test --file test/test.json --raw --wasm-file ../../target/wasm32-unknown-unknown/release-lto/poc-openmeter.wasm


unit_test:
	cargo test -p window-example