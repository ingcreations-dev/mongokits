package db

type MongoOptions struct {
	name string
	server string
	db string
	timeout int
	userName string
	userPass string
}

func (options *MongoOptions) Name(name string) *MongoOptions {
	options.name = name
	return options
}

func (options *MongoOptions) Server(server string) *MongoOptions {
	options.server = server
	return options
}

func (options *MongoOptions) UserName(userName string) *MongoOptions {
	options.userName = userName
	return options
}

func (options *MongoOptions) UserPass(userPass string) *MongoOptions {
	options.userPass = userPass
	return options
}

func (options *MongoOptions) Database(db string) *MongoOptions {
	options.db = db
	return options
}

func (options *MongoOptions) TimeOut(timeout int) *MongoOptions {
	options.timeout = timeout
	return options
}