package com.spriced.Stocking_Certification_Level_update.Service;

import com.spriced.Stocking_Certification_Level_update.Exceptions.PartNotFoundException;
import org.springframework.stereotype.Service;

@Service
public interface StockingCertificationService {
    public String update(String partNumber) throws PartNotFoundException;
}
