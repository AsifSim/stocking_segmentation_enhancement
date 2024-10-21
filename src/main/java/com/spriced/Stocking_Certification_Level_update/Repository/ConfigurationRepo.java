package com.spriced.Stocking_Certification_Level_update.Repository;

import com.spriced.Stocking_Certification_Level_update.Model.Configuration;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface ConfigurationRepo extends JpaRepository<Configuration,Integer> {
    public Optional<Configuration> findByCode(String config);
}
