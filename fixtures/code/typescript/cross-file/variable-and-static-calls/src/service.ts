export class Service {
  start(): void {
    console.log("started");
  }

  stop(): void {
    console.log("stopped");
  }

  static create(): Service {
    return new Service();
  }
}
