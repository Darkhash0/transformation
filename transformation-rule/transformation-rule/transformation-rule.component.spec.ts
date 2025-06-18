import { ComponentFixture, TestBed } from '@angular/core/testing';

import { TransformationRuleComponent } from './transformation-rule.component';

describe('TransformationRuleComponent', () => {
  let component: TransformationRuleComponent;
  let fixture: ComponentFixture<TransformationRuleComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [TransformationRuleComponent]
    })
    .compileComponents();

    fixture = TestBed.createComponent(TransformationRuleComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
