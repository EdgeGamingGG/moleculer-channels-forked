"use strict";

const KafkaAdapter = require("../../src/adapters/kafka");

/**
 * Build a KafkaAdapter instance with the kafkajs client/consumer mocked out,
 * so we can assert on what the adapter passes to `consumer.run()` and whether
 * it performs explicit offset commits — without a real broker.
 */
function createAdapter() {
	const adapter = new KafkaAdapter({ kafka: { brokers: ["localhost:9092"] } });

	// Silence logging
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

	adapter.client = {
		consumer: jest.fn(() => consumer)
	};

	return { adapter, consumer };
}

describe("KafkaAdapter autoCommit option", () => {
	describe("subscribe() wiring", () => {
		it("should default to explicit commit (autoCommit: false) when not set", async () => {
			const { adapter, consumer } = createAdapter();
			const chan = { id: "c1", name: "topic.a", group: "g", handler: jest.fn() };

			await adapter.subscribe(chan);

			expect(chan._autoCommit).toBe(false);
			const runArgs = consumer.run.mock.calls[0][0];
			expect(runArgs.autoCommit).toBe(false);
		});

		it("should enable kafkajs autoCommit when chan.kafka.autoCommit === true", async () => {
			const { adapter, consumer } = createAdapter();
			const chan = {
				id: "c2",
				name: "topic.b",
				group: "g",
				handler: jest.fn(),
				kafka: {
					autoCommit: true,
					autoCommitInterval: 5000,
					autoCommitThreshold: 100
				}
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
			// `"true"` (string) and `1` must NOT enable auto-commit — only boolean true.
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
			const chan = { _autoCommit: true };

			await adapter.maybeCommitOffset(chan, consumer, "topic.a", 0, 42);

			expect(consumer.commitOffsets).not.toHaveBeenCalled();
		});

		it("should commit explicitly when auto-commit is disabled", async () => {
			const { adapter, consumer } = createAdapter();
			const chan = { _autoCommit: false };

			await adapter.maybeCommitOffset(chan, consumer, "topic.a", 0, 42);

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

		it("should await explicit commit per message when auto-commit is off", async () => {
			const { adapter, consumer } = createAdapter();
			adapter.serializer = { deserialize: jest.fn(() => ({})) };
			const chan = createChannel({ _autoCommit: false });
			adapter.initChannelActiveMessages(chan.id);

			await adapter.processMessage(chan, consumer, payload);

			expect(chan.handler).toHaveBeenCalledTimes(1);
			// offset committed is message.offset + 1
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
	});
});
