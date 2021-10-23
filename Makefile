build:
	docker build -t modin-playground .

run:
	docker run -it --rm --shm-size=2.54gb modin-playground
