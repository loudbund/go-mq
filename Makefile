goMq.runcgo : *.go
	sed -i 's/var version = "[0-9]*"/var version = "'`date "+%Y%m%d%H%M%S"`'"/g' main.go
	go build -o goMq.runcgo -ldflags="-s -w" main.go
#	go build -o go_demo_http.runcgo main.go
#main.o : main.c
#	gcc -c main.c
clean :
	rm goMq.runcgo