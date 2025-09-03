// main.ts (for context, no changes needed here if app.config.ts is used)
import { bootstrapApplication } from '@angular/platform-browser';
import { AppComponent } from './app/app.component';
import { appConfig } from './app/app.config';
import 'chartjs-adapter-date-fns';

bootstrapApplication(AppComponent, appConfig).catch((err) =>
  console.error(err)
);
