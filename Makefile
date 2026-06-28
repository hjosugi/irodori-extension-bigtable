.PHONY: build check fmt test package clean

check: fmt test

fmt:
	cargo fmt --check

build:
	cargo build --release

test:
	cargo test

package: build
	mkdir -p dist/native
	cp target/release/libirodori_extension_* dist/native/ 2>/dev/null || true
	cp target/release/irodori_extension_*.dll dist/native/ 2>/dev/null || true
	cp target/release/libirodori_extension_*.dylib dist/native/ 2>/dev/null || true

clean:
	cargo clean
