python-demo-all:
	python3 -m distributed-notebook.demo distributed-notebook/demo/script/script.py distributed-notebook/demo/script/script2.py

python-demo-step1:
	python3 -m distributed-notebook.demo distributed-notebook/demo/script/script.py

python-demo-step2:
	python3 -m distributed-notebook.demo --resume distributed-notebook/demo/script/script2.py

python-demo-step3:
	python3 -m distributed-notebook.demo --resume distributed-notebook/demo/script/script3.py