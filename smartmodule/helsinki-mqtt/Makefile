PROJECT_NAME := window-helsinki
BINARY_NAME := window_helsinki
PROFILE := release-lto

WASM_FILE := ../../target/wasm32-unknown-unknown/$(PROFILE)/$(BINARY_NAME).wasm

build:
	smdk build -p $(PROJECT_NAME) --release $(PROFILE)


test_json: build
	smdk test --file test/test.json --raw --wasm-file $(WASM_FILE)


unit_test:
	cargo test -p $(PROJECT_NAME)


run: build
	fluvio consume --smartmodule-path $(WASM_FILE)  --start 0 --end 1000  helsinki 