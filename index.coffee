{ KafkaConsumer, Producer } = require "node-rdkafka"
_                           = require "underscore"
{ pipeline, Writable }      = require "stream"
through2                    = require "through2"
jsonstream2                  = require "jsonstream2"

config =
	kafka:
		connection:
			"metadata.broker.list" : "localhost:9092"
			"group.id": "le-test-group-id-#{_.random 0, 10000}"
		topicOptions:
			"auto.offset.reset": "earliest"
		flushTimeout: 10000

describe "Node rdkafka", ->
	@timeout 15000
	it "should produce in order", ->
		producer = new Producer config.kafka.connection
			.on "event.error", (error) =>
				console.error "Producer error:", error

		producer.connect()
		await new Promise (res) ->
			producer.once "ready", ->
				res()

		bigData = food: [0..511].map (bla) -> ([0..255].map (char) -> "🥞").join("")
		console.log "approx msg size #{(Buffer.byteLength(JSON.stringify bigData) / 1024).toFixed 2} KB"

		msgs = [0..1000].map (int) ->
			# Generate large message
			Object.assign {}, value: int, bigData

		for msg in msgs
			buffer = Buffer.from JSON.stringify msg
			producer.produce "le-topic", null, buffer

		new Promise (resolve) =>
			producer.flush 10000, (error) ->
				throw new Error "Could not flush producer: #{error.message}" if error
				resolve()

	it "should consume in order", ->
		cs = KafkaConsumer.createReadStream(
			config.kafka.connection
			config.kafka.topicOptions
			topics: [ "le-topic" ]
		)

		prev = 0

		pipeline [
			cs
			through2.obj (obj, enc, cb) -> cb null, obj.value
			jsonstream2.parse()
			new Writable
				objectMode: true
				write: (obj, enc, cb) ->
					console.log('obj', obj.value)
					return cb new Error "Ouch! #{obj.value} < #{prev}" if obj.value < prev and obj.value isnt 0
					prev = obj.value
					cb()

		], (error) ->
			throw error if error


