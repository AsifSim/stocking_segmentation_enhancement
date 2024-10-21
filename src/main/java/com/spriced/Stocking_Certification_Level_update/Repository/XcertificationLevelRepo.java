package com.spriced.Stocking_Certification_Level_update.Repository;

import com.spriced.Stocking_Certification_Level_update.Model.XcertificationLevel;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface XcertificationLevelRepo extends JpaRepository<XcertificationLevel,Integer> {
}
