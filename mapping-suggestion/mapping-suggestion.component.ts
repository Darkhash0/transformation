import { Component, OnInit } from '@angular/core';
import { LoaderComponent } from '../../shared/loader/loader.component';
import { MatCheckboxModule } from '@angular/material/checkbox';
import { ActivatedRoute, Router, NavigationExtras } from '@angular/router';
import { NgIf } from '@angular/common';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { MatButtonModule } from '@angular/material/button';
import { MatIconModule } from '@angular/material/icon';
import { BreadcrumbService } from '../breadcrumb.service';
import { MatFormFieldModule, MatLabel } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { AgGridModule } from 'ag-grid-angular';
import { ColDef, GridApi, GridReadyEvent } from 'ag-grid-community';
import { HttpClient } from '@angular/common/http';
import { MessageDialogComponent } from '../../shared/message-dialog/message-dialog.component';
import { MatDialog } from '@angular/material/dialog';
import { MyActionCellComponent } from '../transformation-config/grid/my-action-cell.component';

@Component({
  selector: 'app-mapping-suggestion',
  imports: [
    LoaderComponent,
    CommonModule,
    FormsModule,
    MatCheckboxModule,
    MatButtonModule,
    NgIf,
    MatIconModule,
    MatFormFieldModule,
    MatLabel,
    MatInputModule,
    AgGridModule,
  ],
  templateUrl: './mapping-suggestion.component.html',
  styleUrl: './mapping-suggestion.component.css',
})
export class MappingSuggestionComponent implements OnInit {
  isMappingConfirmed: boolean = false;
  loader: boolean = false;
  breadcrumbs: any;
  currentIndex: number = 0;
  sourceModelPath: any;
  targetPath: any;
  autoname: any;
  configNames: any = [];
  duplicateField: boolean = false;
  isMappingGenerated = false;
  seletedRowsCount = 0;
  selectedValue : any;
  isEdited = false;
  isSaveDisabled: boolean = true;
  isExecuteDisabled: boolean = false;


  constructor(
    private router: Router,
    private route: ActivatedRoute,
    private breadcrumbService: BreadcrumbService,
    private http: HttpClient,
    private dialog: MatDialog,
  ) {}

  // routeToMapping() {
  //   window.open('http://172.16.4.37:4200/repositories', '_blank');
  // }
  // nextToLoad() {
  //   this.router.navigate(['/migration-services/transformation-config']);
  // }

  rowData: any[] = [];
  private gridApi!: GridApi;

  openDialog(title: string, message: string) {
    this.dialog.open(MessageDialogComponent, {
      data: { title, message },
    });
  }

  columnDefs: ColDef[] = [
    {
      headerCheckboxSelection: true,
      checkboxSelection: true,
      width: 50,
      pinned: 'left'
    },
    { headerName: 'Config Name', field: 'config_name', sortable: true, filter: true },
    // { headerName: 'Repo Type', field: 'repo_type', sortable: true, filter: true, editable: true },
    { headerName: 'Source Model File', field: 'source_model_path', sortable: true, filter: true, editable: true },
    { headerName: 'Target Model File', field: 'target_path', sortable: true, filter: true, editable: true },
    { headerName: 'Mapping Generated', field: 'mapping_generated', sortable: true, filter: true, editable: true },
    {
      headerName: 'Actions',
      cellRenderer: MyActionCellComponent,
      cellRendererParams: {
        onEdit: this.onEdit.bind(this),
        onDelete: this.onDelete.bind(this),
      },
      editable: false,
      colId: 'actions',
    },
  ];

  onEdit(rowData: any) {
    console.log(rowData, 'edit');
  }
  onDelete(rowData: any) {
    this.isEdited = true;
    this.isSaveDisabled = false;
    this.isExecuteDisabled = true
    console.log(rowData, 'delete');
    this.rowData = this.rowData.filter((row: any) => row.id !== rowData.id);
    console.log(this.rowData);
  }

  defaultColDef: ColDef = {
    resizable: true,
    flex: 1,
  };

  ngOnInit(): void {

    this.fetchRowData();

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

  duplicateCheck() {
    this.duplicateField = this.configNames.includes(this.autoname.trim());
  }

  fetchRowData(): void {
    this.http.get<any>('retrieve_configuration/').subscribe({
          next: (res) => {
            console.log(res,'res');
            const configs = res.config_data_before?.configurations;
            console.log(configs,'configs');
            this.rowData = configs || [];
          },
          error: (err) => {
            console.error('Failed to fetch config data', err);
          }
        });
      }

  onGridReady(params: GridReadyEvent): void {
          this.gridApi = params.api;
          this.gridApi.sizeColumnsToFit();
          params.api.addEventListener('selectionChanged', () => {
            this.seletedRowsCount = params.api.getSelectedRows().length;
          });
        }

  generateButtonEnabled(): boolean {
    return(
      (this.autoname && this.sourceModelPath && this.targetPath) || this.seletedRowsCount > 0);
  }

  onSave(): void {
    this.loader = true;
    const newRow = {
      configname: this.autoname,
      // repoType: '',
      source_modelfile: this.sourceModelPath,
      target_modelfile: this.targetPath
    };

    // this.rowData = [...this.rowData, newRow];
this.http.post('save_configuration/', newRow).subscribe({
  next: (response) => {
    this.loader = false;
    this.rowData = (response as any).config_data_after || [];
        this.fetchRowData();

        this.autoname = '';
    this.sourceModelPath = '';
    this.targetPath = '';
    this.openDialog('Success', 'Save was successful!');

    this.isMappingGenerated = false;

  },
    error: (err) => {
      this.loader = false;
      this.openDialog('Error', 'Save failed!');
      console.error('Failed to Save Config data', err);
    }
  });
  }


  generateMapping(): void {
    this.loader = false;
    if (!this.generateButtonEnabled()) return;

    const selectedRows = this.gridApi.getSelectedRows();

    // if (selectedRows.length === 0) {
    //   alert('Please select a configuration to generate mapping.');
    //   return;
    // }

    const selectedConfig = selectedRows[0];

    const requestBody = {
      sourceModelPath: selectedConfig.source_model_path,
      tgtFilepath: selectedConfig.target_path,
      // dataChk: false,
      id: selectedConfig.id
    };
    console.log(requestBody,'requestbody');
    console.log(this.rowData,'rowdata');
    this.selectedValue = selectedConfig;
    console.log(selectedConfig,'seslected row');


    this.http.post<any>('generate_mapping/', requestBody).subscribe({
      next: (res) => {
        this.loader = false;
        console.log('Mapping generated:', res);
        this.openDialog('Success', 'Generate Mapping with AI is completed!');
        this.isMappingGenerated = true;

        sessionStorage.setItem('generatedMappingData',JSON.stringify(res.message?.mapped_data || []));
      },
      error: (err) => {
        this.loader = false;
        this.openDialog('Error', 'Generate Mapping with AI failed!');
        console.error('Mapping generation failed:', err);
      }
    });
  }


  nextToLoad(): void {
    this.isMappingGenerated = true;
    if(this.isMappingGenerated){

    const savedData = sessionStorage.getItem('breadcrumbData');

    if (!savedData) return;
    const breadcrumbs = JSON.parse(savedData);
    const currentUrl = this.router.url.split('/').pop();
    const currentIndex = breadcrumbs.findIndex(
      (b: any) => b.route === currentUrl
    );
    let navigationExtras : NavigationExtras = {
        state: this.selectedValue,
      };
    if (currentIndex !== -1 && currentIndex < breadcrumbs.length - 1) {
      const nextRoute = breadcrumbs[currentIndex + 1].route;
      this.router.navigate(['../' + nextRoute],
         {relativeTo: this.route , ...navigationExtras});
    }
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
      this.router.navigate(['../' + previousRoute], { relativeTo: this.route });
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
