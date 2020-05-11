init-demo:
	rm -rf examples/env && virtualenv --py=python3 examples/env && examples/env/bin/pip3 install -r examples/reqs.txt

uninstall:
	yes y | examples/env/bin/pip3 uninstall dataloader

cleanup: uninstall
	rm -rf dist/ && rm -rf build/ && rm -rf src/DataLoader.egg-info && rm -rf examples/src/target

dist: cleanup
	examples/env/bin/python3 setup.py sdist

unzip: dist
	cd dist && tar -xzvf DataLoader-*.tar.gz && cd ..

install: unzip
	examples/env/bin/python dist/DataLoader-*/setup.py install && rm -rf src/DataLoader.egg-info

run-demo: install
	clear && examples/env/bin/python examples/src/run.py