const amqp = require("amqplib");

class MessageBroker {
  constructor() {
    this.channel = null;
  }

  async connect() {
    console.log("Connecting to RabbitMQ...");

    // Đọc URI từ biến môi trường (được Docker cung cấp)
    const rabbitURI = process.env.RABBITMQ_URI || "amqp://admin:123456@rabbitmq:5672";

    setTimeout(async () => {
      try {
        const connection = await amqp.connect(rabbitURI);
        this.channel = await connection.createChannel();
        await this.channel.assertQueue("products");
        console.log("✅ RabbitMQ connected successfully");
      } catch (err) {
        console.error("❌ Failed to connect to RabbitMQ:", err.message);
      }
    }, 10000); // chờ 10 giây để RabbitMQ sẵn sàng
  }

  async publishMessage(queue, message) {
    if (!this.channel) {
      console.error("No RabbitMQ channel available.");
      return;
    }

    try {
      await this.channel.sendToQueue(queue, Buffer.from(JSON.stringify(message)));
    } catch (err) {
      console.error("Publish message failed:", err);
    }
  }

  async consumeMessage(queue, callback) {
    if (!this.channel) {
      console.error("No RabbitMQ channel available.");
      return;
    }

    try {
      await this.channel.consume(queue, (message) => {
        const content = message.content.toString();
        const parsedContent = JSON.parse(content);
        callback(parsedContent);
        this.channel.ack(message);
      });
    } catch (err) {
      console.error("Consume message failed:", err);
    }
  }
}

module.exports = new MessageBroker();
