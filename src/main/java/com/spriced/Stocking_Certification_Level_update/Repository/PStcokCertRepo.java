package com.spriced.Stocking_Certification_Level_update.Repository;

import com.spriced.Stocking_Certification_Level_update.Model.PStockCert;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface PStcokCertRepo extends JpaRepository<PStockCert,Integer> {
    public Optional<PStockCert> findByCertification(String certification);
}
