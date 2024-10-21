package com.spriced.Stocking_Certification_Level_update.Repository;

import com.spriced.Stocking_Certification_Level_update.Model.Part;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface PartRepo extends JpaRepository<Part,Integer> {
    public Optional<Part> findByCode(String code);
}
