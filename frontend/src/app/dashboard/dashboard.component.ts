// src/app/dashboard/dashboard.component.ts
import { Component, OnInit, OnDestroy, ChangeDetectorRef } from '@angular/core';
import { CommonModule, DatePipe } from '@angular/common';
import { Subscription } from 'rxjs';
import { WebsocketService, StockData } from '../services/websocket.service';

// Import the chart directive and types
import { BaseChartDirective } from 'ng2-charts';
import { ChartConfiguration, ChartOptions, ChartType } from 'chart.js';

// Define a type for our chart objects to keep things organized
interface StockChart {
  symbol: string;
  chartData: ChartConfiguration['data'];
  chartOptions: ChartOptions;
  chartType: ChartType;
}

@Component({
  selector: 'app-dashboard',
  standalone: true,
  // Import the necessary modules for a standalone component
  imports: [CommonModule, BaseChartDirective],
  providers: [DatePipe], // Add DatePipe for formatting timestamps
  templateUrl: './dashboard.component.html',
  styleUrls: ['./dashboard.component.css'],
})
export class DashboardComponent implements OnInit, OnDestroy {
  // Use a Map to store charts, allowing for efficient updates and ordering
  public charts = new Map<string, StockChart>();
  private dataSubscription: Subscription | undefined;
  private readonly MAX_DATA_POINTS = 30; // Max data points to show on the chart

  constructor(
    private websocketService: WebsocketService,
    private datePipe: DatePipe,
    private cdr: ChangeDetectorRef // Inject ChangeDetectorRef for manual change detection
  ) {}

  ngOnInit(): void {
    this.websocketService.messages$.subscribe((stock) => {
      this.updateCharts(stock);
    });
  }

  private updateCharts(stock: StockData): void {
    const formattedTime =
      this.datePipe.transform(stock.timestamp, 'mediumTime') || '';

    if (this.charts.has(stock.symbol)) {
      // If chart exists, update it
      const existingChart = this.charts.get(stock.symbol)!;
      const dataArray = existingChart.chartData.datasets[0].data;
      const labels = existingChart.chartData.labels!;

      // Add new data
      dataArray.push(stock.price);
      labels.push(formattedTime);

      // Maintain a sliding window of data points
      if (dataArray.length > this.MAX_DATA_POINTS) {
        dataArray.shift();
        labels.shift();
      }
    } else {
      // If chart is new, create it
      const newChart = this.createStockChart(
        stock.symbol,
        stock.price,
        formattedTime
      );
      this.charts.set(stock.symbol, newChart);
    }
    // Manually trigger change detection as chart data objects are mutated
    this.cdr.detectChanges();
  }

  private createStockChart(
    symbol: string,
    initialPrice: number,
    initialTime: string
  ): StockChart {
    return {
      symbol: symbol,
      chartType: 'line',
      chartOptions: this.getChartOptions(symbol),
      chartData: {
        labels: [initialTime],
        datasets: [
          {
            data: [initialPrice],
            label: `${symbol} Price`,
            borderColor: '#4ecca3',
            backgroundColor: 'rgba(78, 204, 163, 0.3)',
            pointBackgroundColor: '#ffffff',
            pointBorderColor: '#4ecca3',
            pointRadius: 4,
            fill: true,
            tension: 0.4, // Makes the line smooth
          },
        ],
      },
    };
  }

  private getChartOptions(symbol: string): ChartOptions {
    return {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: {
          display: false, // We'll show the symbol in the card header
        },
      },
      scales: {
        x: {
          ticks: { color: '#a0a0a0' },
          grid: { color: 'rgba(255, 255, 255, 0.1)' },
        },
        y: {
          ticks: {
            color: '#a0a0a0',
            // Format the y-axis ticks as currency
            callback: function (value) {
              return '$' + Number(value).toFixed(2);
            },
          },
          grid: { color: 'rgba(255, 255, 255, 0.1)' },
        },
      },
    };
  }

  ngOnDestroy(): void {
    if (this.dataSubscription) {
      this.dataSubscription.unsubscribe();
    }
  }

  getLatestData(chartData: ChartConfiguration['data']): number {
    // Access the data from the first dataset, as Chart.js data structure is datasets: [{ data: [...] }]
    const dataset = chartData.datasets[0];
    if (!dataset || dataset.data.length === 0) return 0;
    const lastestData = dataset.data[dataset.data.length - 1];
    if (typeof lastestData === 'number') {
      // Ensure the data point is a number
      return lastestData; // Return the last data point
    }
    return 0; // Default to 0 if no valid data is found
  }
}
