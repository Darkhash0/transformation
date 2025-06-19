import { ComponentFixture, TestBed } from '@angular/core/testing';

import { MappingSuggestionComponent } from './mapping-suggestion.component';

describe('MappingSuggestionComponent', () => {
  let component: MappingSuggestionComponent;
  let fixture: ComponentFixture<MappingSuggestionComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [MappingSuggestionComponent]
    })
    .compileComponents();

    fixture = TestBed.createComponent(MappingSuggestionComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
