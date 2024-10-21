package com.spriced.Stocking_Certification_Level_update.Model;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Part {
    @Id
    Integer id;
    Integer certificationLevel;
    String code;
    String suggestedStockingCert;
//    String configuration;
//    String stockingCertificationLevel;
    String productBu;
}
