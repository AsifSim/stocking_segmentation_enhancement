package com.spriced.Stocking_Certification_Level_update.Model;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import lombok.*;

@Setter
@Getter
@Entity
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Configuration {
    @Id
    Integer id;
    Integer certificationLevel;
    String code;
    String stockingCertificationLevel;
}
