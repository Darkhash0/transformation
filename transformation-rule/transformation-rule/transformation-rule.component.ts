import { Component } from '@angular/core';
import { LoaderComponent } from '../../shared/loader/loader.component';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { MatFormFieldModule, MatLabel } from '@angular/material/form-field';
import { MatOptionModule } from '@angular/material/core';
import { MatSelectModule } from '@angular/material/select';
import { MatInputModule } from '@angular/material/input';
import { MatDialog } from '@angular/material/dialog';
import { HttpClient } from '@angular/common/http';
import { ActivatedRoute, Router } from '@angular/router';
import { BreadcrumbService } from '../breadcrumb.service';
import { MessageDialogComponent } from '../../shared/message-dialog/message-dialog.component';
import { MatIconModule } from '@angular/material/icon';

@Component({
  selector: 'app-transformation-rule',
  imports: [
    LoaderComponent,
    CommonModule,
    FormsModule,
    MatFormFieldModule,
    MatOptionModule,
    MatSelectModule,
    MatSelectModule,
    MatLabel,
    MatInputModule,
    MatIconModule,
  ],
  templateUrl: './transformation-rule.component.html',
  styleUrl: './transformation-rule.component.css',
})
export class TransformationRuleComponent {
  loader: boolean = false;
  targetColumnName: any = '';
  scourceColumn: any = '';
  selectedRuleSelector: any = '';
  instructions: any = '';
  isNextDisabled = false;
  breadcrumbs: any;
  currentIndex: number = 0;
  sourceColumns: string[] = ['PersonInd'];

  constructor(
    private dialog: MatDialog,
    private http: HttpClient,
    private router: Router,
    private route: ActivatedRoute,
    private breadcrumbService: BreadcrumbService
  ) {}

  openDialog(title: string, message: string): void {
    const dialogRef = this.dialog.open(MessageDialogComponent, {
      data: { title, message },
    });
  }

  onSubmit() {
    this.http.get('/').subscribe({
      next: (res) => {
        this.loader = false;
        this.isNextDisabled = true;
        this.openDialog('Success', 'Success! .');
      },
      error: (err) => {
        this.loader = false;
        this.isNextDisabled = false;
        this.openDialog('Error', 'Failed! Error .');
      },
    });
  }

  ngOnInit(): void {
    // const savedData = localStorage.getItem('breadcrumbData');
    const savedData = sessionStorage.getItem('breadcrumbData');

    if (savedData) {
      this.breadcrumbs = JSON.parse(savedData);
    } else {
      this.breadcrumbs = this.breadcrumbService.breadcrumbs;
    }

    if (this.breadcrumbs.length > 0) {
      const currentUrl = this.router.url.split('/').pop(); // Get the current route path
      const matchedIndex = this.breadcrumbs.findIndex(
        (b: any) => b.route === currentUrl
      );

      if (matchedIndex !== -1) {
        this.currentIndex = matchedIndex;
      } else {
        this.currentIndex = 0;
        this.router.navigate([this.breadcrumbs[0].route], {
          relativeTo: this.route,
        });
      }
    }
  }

  addSourceColumn(): void {
    this.sourceColumns.push('');
  }

  onNext(): void {
    // const savedData = localStorage.getItem('breadcrumbData');
    const savedData = sessionStorage.getItem('breadcrumbData');

    if (!savedData) return;

    const breadcrumbs = JSON.parse(savedData);
    const currentUrl = this.router.url.split('/').pop();

    const currentIndex = breadcrumbs.findIndex(
      (b: any) => b.route === currentUrl
    );
    if (currentIndex !== -1 && currentIndex < breadcrumbs.length - 1) {
      const nextRoute = breadcrumbs[currentIndex + 1].route;
      this.router.navigate(['../' + nextRoute], { relativeTo: this.route });
    }
  }
  onPrevious() {
    // const savedData = localStorage.getItem('breadcrumbData');
    const savedData = sessionStorage.getItem('breadcrumbData');
    if (!savedData) {
      this.router.navigateByUrl('smart-migrator-home');
      return;
    }

    const breadcrumbs = JSON.parse(savedData);
    const currentUrl = this.router.url.split('/').pop();
    const currentIndex = breadcrumbs.findIndex(
      (b: any) => b.route === currentUrl
    );

    if (currentIndex > 0) {
      const previousRoute = breadcrumbs[currentIndex - 1].route;
      this.router.navigate(['../' + previousRoute], {
        relativeTo: this.route,
        state: { reload: true },
      });
    } else {
      this.router.navigateByUrl('smart-migrator-home');
    }
  }
  onFinish(): void {
    this.router.navigateByUrl('smart-migrator-home');
  }
  isLastBreadcrumb(): boolean {
    return this.currentIndex === this.breadcrumbs.length - 1;
  }
}
