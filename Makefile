SRC		= src/main.go src/model.go

start :
		@go run $(SRC)


.PHONY:start