unit_test:
	cargo test -p fluvio-smartmodule-window
	make -C smartmodule/helsinki-mqtt unit_test
#	make -C poc/openmeter unit_test