package com.spriced.Stocking_Certification_Level_update.Model;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Entity
@Data
@NoArgsConstructor
@AllArgsConstructor
@Getter
public class XcertificationLevel {
    @Id
    Integer id;
    String name;
}
