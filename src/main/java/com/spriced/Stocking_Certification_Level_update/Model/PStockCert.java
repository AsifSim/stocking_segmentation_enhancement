package com.spriced.Stocking_Certification_Level_update.Model;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Data
@NoArgsConstructor
@AllArgsConstructor
@Table(name="p_stock_cert")
public class PStockCert {
    @Id
    Integer code;
    String certification;
    Integer ebuRank;
    Integer hhpRank;
}
