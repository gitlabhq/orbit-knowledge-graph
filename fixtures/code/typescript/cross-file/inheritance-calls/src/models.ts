class Animal {
  speak(): string {
    return "...";
  }
}

class Dog extends Animal {
  speak(): string {
    return `${super.speak()} woof`;
  }
}

class Base {
  helper(): number {
    return 1;
  }
}

class Child extends Base {
  run(): number {
    return this.helper();
  }
}
