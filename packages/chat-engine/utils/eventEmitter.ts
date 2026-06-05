export default class SimpleEventEmitter {
  private events: Map<string, Array<(...args: any[]) => void>> = new Map();

  on(event: string, listener: (...args: any[]) => void): void {
    if (!this.events.has(event)) {
      this.events.set(event, []);
    }
    this.events.get(event)!.push(listener);
  }

  off(event: string, listener: (...args: any[]) => void): void {
    const listeners = this.events.get(event);
    if (!listeners) return;

    const index = listeners.indexOf(listener);
    if (index !== -1) {
      listeners.splice(index, 1);
    }
  }

  once(event: string, listener: (...args: any[]) => void): void {
    const wrapper = (...args: any[]) => {
      this.off(event, wrapper);
      listener(...args);
    };
    this.on(event, wrapper);
  }

  emit(event: string, ...args: any[]): boolean {
    const listeners = this.events.get(event);
    if (!listeners || listeners.length === 0) return false;

    listeners.forEach((listener) => {
      try {
        listener(...args);
      } catch (error) {
        console.error('EventEmitter listener error:', error);
      }
    });
    return true;
  }

  removeAllListeners(event?: string): void {
    if (event) {
      this.events.delete(event);
    } else {
      this.events.clear();
    }
  }
}
