"use strict";

const { ServiceBroker } = require("moleculer");
const ChannelMiddleware = require("./../../").Middleware;
const KafkaAdapter = require("../../src/adapters/kafka");
const C = require("../../src/constants");

describe("Test service 'channelHandlerTrigger' method", () => {
	const serviceSchema = {
		name: "helper",

		channels: {
			async "helper.sum"(payload) {
				// Calls the sum method
				return this.sum(payload.a, payload.b);
			},

			"helper.subtract": {
				handler(payload) {
					return this.subtract(payload.a, payload.b);
				}
			}
		},

		methods: {
			sum(a, b) {
				return a + b;
			},

			subtract(a, b) {
				return a - b;
			}
		}
	};

	describe("Test service default value", () => {
		let broker = new ServiceBroker({
			logger: false,
			middlewares: [
				ChannelMiddleware({
					adapter: {
						type: "Fake"
					}
				})
			]
		});
		let service = broker.createService(serviceSchema);
		beforeAll(() => broker.start());
		afterAll(() => broker.stop());

		it("should register default 'emitLocalChannelHandler' function declaration", async () => {
			// Mock the "sum" method
			service.sum = jest.fn();

			// Call the "helper.sum" handler
			await service.emitLocalChannelHandler("helper.sum", { a: 5, b: 5 });
			// Check if "sum" method was called
			expect(service.sum).toBeCalledTimes(1);
			expect(service.sum).toBeCalledWith(5, 5);

			// Restore the "sum" method
			service.sum.mockRestore();
		});

		it("should register default 'emitLocalChannelHandler' object declaration", async () => {
			// Mock the "sum" method
			service.subtract = jest.fn();

			// Call the "helper.sum" handler
			await service.emitLocalChannelHandler("helper.subtract", { a: 5, b: 5 });
			// Check if "subtract" method was called
			expect(service.subtract).toBeCalledTimes(1);
			expect(service.subtract).toBeCalledWith(5, 5);

			// Restore the "subtract" method
			service.subtract.mockRestore();
		});
	});

	describe("Test service custom value", () => {
		let broker = new ServiceBroker({
			logger: false,
			middlewares: [
				ChannelMiddleware({
					channelHandlerTrigger: "myTrigger",
					adapter: {
						type: "Fake"
					}
				})
			]
		});
		let service = broker.createService(serviceSchema);
		beforeAll(() => broker.start());
		afterAll(() => broker.stop());

		it("should register with 'myTrigger'", async () => {
			// Mock the "sum" method
			service.sum = jest.fn();

			// Call the "helper.sum" handler
			await service.myTrigger("helper.sum", { a: 5, b: 5 });
			// Check if "sum" method was called
			expect(service.sum).toBeCalledTimes(1);
			expect(service.sum).toBeCalledWith(5, 5);

			// Restore the "sum" method
			service.sum.mockRestore();
		});
	});
});

describe("Test KafkaAdapter autoCommit option", () => {
	/**
	 * Build a KafkaAdapter with the kafkajs client/consumer mocked out, so we can
	 * assert on what the adapter passes to `consumer.run()` and whether it performs
	 * explicit offset commits — without a real broker.
	 */
	function createAdapter() {
		const adapter = new KafkaAdapter({ kafka: { brokers: ["localhost:9092"] } });

		adapter.logger = {
			debug: jest.fn(),
			info: jest.fn(),
			warn: jest.fn(),
			error: jest.fn()
		};

		const consumer = {
			connect: jest.fn(async () => {}),
			subscribe: jest.fn(async () => {}),
			run: jest.fn(async () => {}),
			commitOffsets: jest.fn(async () => {})
		};

		adapter.client = { consumer: jest.fn(() => consumer) };

		return { adapter, consumer };
	}

	describe("subscribe() wiring", () => {
		it("should default to explicit commit (autoCommit: false) when not set", async () => {
			const { adapter, consumer } = createAdapter();
			const chan = { id: "c1", name: "topic.a", group: "g", handler: jest.fn() };

			await adapter.subscribe(chan);

			expect(chan._autoCommit).toBe(false);
			expect(consumer.run.mock.calls[0][0].autoCommit).toBe(false);
		});

		it("should enable kafkajs autoCommit when chan.kafka.autoCommit === true", async () => {
			const { adapter, consumer } = createAdapter();
			const chan = {
				id: "c2",
				name: "topic.b",
				group: "g",
				handler: jest.fn(),
				kafka: { autoCommit: true, autoCommitInterval: 5000, autoCommitThreshold: 100 }
			};

			await adapter.subscribe(chan);

			expect(chan._autoCommit).toBe(true);
			const runArgs = consumer.run.mock.calls[0][0];
			expect(runArgs.autoCommit).toBe(true);
			expect(runArgs.autoCommitInterval).toBe(5000);
			expect(runArgs.autoCommitThreshold).toBe(100);
		});

		it("should treat any non-`true` autoCommit value as false (strict check)", async () => {
			const { adapter, consumer } = createAdapter();
			const chan = {
				id: "c3",
				name: "topic.c",
				group: "g",
				handler: jest.fn(),
				kafka: { autoCommit: "true" }
			};

			await adapter.subscribe(chan);

			expect(chan._autoCommit).toBe(false);
			expect(consumer.run.mock.calls[0][0].autoCommit).toBe(false);
		});
	});

	describe("maybeCommitOffset()", () => {
		it("should NOT commit explicitly when channel uses auto-commit", async () => {
			const { adapter, consumer } = createAdapter();

			await adapter.maybeCommitOffset({ _autoCommit: true }, consumer, "topic.a", 0, 42);

			expect(consumer.commitOffsets).not.toHaveBeenCalled();
		});

		it("should commit explicitly when auto-commit is disabled", async () => {
			const { adapter, consumer } = createAdapter();

			await adapter.maybeCommitOffset({ _autoCommit: false }, consumer, "topic.a", 0, 42);

			expect(consumer.commitOffsets).toHaveBeenCalledTimes(1);
			expect(consumer.commitOffsets).toHaveBeenCalledWith([
				{ topic: "topic.a", partition: 0, offset: 42 }
			]);
		});
	});

	describe("processMessage() commit behavior", () => {
		function createChannel(overrides) {
			return Object.assign(
				{
					id: "c1",
					name: "topic.a",
					group: "g",
					maxRetries: 0,
					deadLettering: { enabled: false },
					handler: jest.fn(async () => {})
				},
				overrides
			);
		}

		const payload = {
			topic: "topic.a",
			partition: 0,
			message: { offset: "10", headers: {}, value: Buffer.from("{}") }
		};

		it("should await explicit commit of offset+1 per message when auto-commit is off", async () => {
			const { adapter, consumer } = createAdapter();
			adapter.serializer = { deserialize: jest.fn(() => ({})) };
			const chan = createChannel({ _autoCommit: false });
			adapter.initChannelActiveMessages(chan.id);

			await adapter.processMessage(chan, consumer, payload);

			expect(chan.handler).toHaveBeenCalledTimes(1);
			expect(consumer.commitOffsets).toHaveBeenCalledWith([
				{ topic: "topic.a", partition: 0, offset: 11 }
			]);
		});

		it("should skip explicit commit per message when auto-commit is on", async () => {
			const { adapter, consumer } = createAdapter();
			adapter.serializer = { deserialize: jest.fn(() => ({})) };
			const chan = createChannel({ _autoCommit: true });
			adapter.initChannelActiveMessages(chan.id);

			await adapter.processMessage(chan, consumer, payload);

			expect(chan.handler).toHaveBeenCalledTimes(1);
			expect(consumer.commitOffsets).not.toHaveBeenCalled();
		});

		// The remaining branches of processMessage (group-skip, drop, retry) each
		// also acknowledge via maybeCommitOffset. These guard against a regression
		// where one branch reverts to calling commitOffset() directly — which would
		// commit even when the channel opted into auto-commit. Asserting "no commit
		// when auto-commit is on" pins the routing for each branch.

		it("should route the group-skip path through the gate (no commit when auto-commit on)", async () => {
			const { adapter, consumer } = createAdapter();
			adapter.serializer = { deserialize: jest.fn(() => ({})) };
			const chan = createChannel({ _autoCommit: true });
			adapter.initChannelActiveMessages(chan.id);

			// Message addressed to a different group than the channel's → skipped.
			const skipPayload = {
				topic: "topic.a",
				partition: 0,
				message: {
					offset: "10",
					headers: { [C.HEADER_GROUP]: Buffer.from("other-group") },
					value: Buffer.from("{}")
				}
			};

			await adapter.processMessage(chan, consumer, skipPayload);

			expect(chan.handler).not.toHaveBeenCalled();
			expect(consumer.commitOffsets).not.toHaveBeenCalled();
		});

		it("should commit offset+1 on the drop path (no retries) when auto-commit is off", async () => {
			const { adapter, consumer } = createAdapter();
			adapter.serializer = { deserialize: jest.fn(() => ({})) };
			adapter.metricsIncrement = jest.fn();
			const chan = createChannel({
				_autoCommit: false,
				maxRetries: 0,
				handler: jest.fn(async () => {
					throw new Error("boom");
				})
			});
			adapter.initChannelActiveMessages(chan.id);

			await adapter.processMessage(chan, consumer, payload);

			expect(consumer.commitOffsets).toHaveBeenCalledWith([
				{ topic: "topic.a", partition: 0, offset: 11 }
			]);
		});

		it("should route the drop path (no retries) through the gate (no commit when auto-commit on)", async () => {
			const { adapter, consumer } = createAdapter();
			adapter.serializer = { deserialize: jest.fn(() => ({})) };
			adapter.metricsIncrement = jest.fn();
			const chan = createChannel({
				_autoCommit: true,
				maxRetries: 0,
				handler: jest.fn(async () => {
					throw new Error("boom");
				})
			});
			adapter.initChannelActiveMessages(chan.id);

			await adapter.processMessage(chan, consumer, payload);

			expect(consumer.commitOffsets).not.toHaveBeenCalled();
		});

		it("should route the retry/redeliver path through the gate (no commit when auto-commit on)", async () => {
			const { adapter, consumer } = createAdapter();
			adapter.serializer = { deserialize: jest.fn(() => ({})) };
			adapter.metricsIncrement = jest.fn();
			adapter.publish = jest.fn(async () => {}); // redelivery republishes the message
			const chan = createChannel({
				_autoCommit: true,
				maxRetries: 2,
				handler: jest.fn(async () => {
					throw new Error("boom");
				})
			});
			adapter.initChannelActiveMessages(chan.id);

			await adapter.processMessage(chan, consumer, payload);

			expect(adapter.publish).toHaveBeenCalledTimes(1); // confirms we took the retry branch
			expect(consumer.commitOffsets).not.toHaveBeenCalled();
		});
	});
});
