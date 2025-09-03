import { Injectable, OnDestroy } from '@angular/core';
import { EMPTY, NEVER, Observable, Subject, timer } from 'rxjs';
import { webSocket } from 'rxjs/webSocket';
import { catchError, mergeMap, repeat, takeUntil, tap } from 'rxjs/operators';

export interface StockData {
  price: number;
  symbol: string;
  volume: number;
  timestamp: number;
}

@Injectable({
  providedIn: 'root',
})
export class WebsocketService implements OnDestroy {
  private messagesSubject = new Subject<StockData>();
  private destroy$ = new Subject<void>();

  public messages$ = this.messagesSubject.asObservable();

  constructor() {
    this.connect();
  }

  private connect(): void {
    // The source now emits arrays of StockData, as a single websocket message
    // might contain multiple data points.
    const source$ = new Observable<StockData[]>((observer) => {
      console.log('Connecting to WebSocket...');
      const socket$ = webSocket<StockData[]>({
        url: 'ws://localhost:8080/ws',
        deserializer: (e: MessageEvent) => {
          if (!e.data) {
            return [];
          }
          // A single message from the server can contain multiple concatenated JSON objects.
          // e.g. {"price":1...}{"price":2...}
          // We need to make this a valid JSON array string: [{"price":1...},{"price":2...}]
          // First add comma between the curly braces e.g. }{ becomes },{
          const data = e.data.replace(/}\s*{/g, '},{');
          const jsonArrayStr = `[${data}]`;
          try {
            // This will parse an array of stock data objects.
            return JSON.parse(jsonArrayStr);
          } catch (err) {
            console.error('Error parsing WebSocket message chunk:', err);
            console.error('Original message data:', e.data);
            return []; // Return empty array on parsing error to not break the stream.
          }
        },
      });
      const subscription = socket$.subscribe(observer);
      return () => {
        // On unsubscription, the subscription to the underlying socket is unsubscribed.
        // The WebSocketSubject will automatically close the connection when it has no more subscribers.
        subscription.unsubscribe();
      };
    });

    source$
      .pipe(
        // Flatten the array of messages into a stream of single messages.
        mergeMap((messages) => messages),
        // The original implementation used separate retry/repeat logic.
        // This can be simplified and made more robust by normalizing all
        // connection-ending events (error or clean close) into a single
        // path that the `repeat` operator can handle.
        tap({
          // Log clean completions, which can happen if the server closes the connection.
          complete: () => console.log('WebSocket connection closed cleanly.'),
        }),
        catchError((err) => {
          // `catchError` handles connection errors.
          // Instead of retrying here, we log the error and return EMPTY.
          // EMPTY completes the stream, allowing `repeat` to take over.
          console.error('WebSocket error:', err);
          return EMPTY;
        }),
        repeat({
          // `repeat` will resubscribe on any completion, whether it was a
          // clean close or an error that was converted to a completion by `catchError`.
          delay: (count) => {
            console.log(`Attempting to reconnect (attempt ${count})...`);
            return timer(1000); // Delay before resubscribing.
          },
        }),
        // This final `catchError` is a safeguard. It will only be reached if the
        // `repeat` operator itself fails, which is highly unlikely.
        catchError((err) => {
          console.error('Subscription error, stream will not be retried:', err);
          // Return an observable that never emits or completes,
          // effectively stopping the stream without erroring the main subject.
          return NEVER;
        }),
        takeUntil(this.destroy$)
      )
      .subscribe(this.messagesSubject);
  }

  public close(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }

  ngOnDestroy(): void {
    this.close();
  }
}
