package com.sparta.company.domain.model

import com.fasterxml.jackson.annotation.JsonFormat
import jakarta.persistence.Column
import jakarta.persistence.EntityListeners
import jakarta.persistence.MappedSuperclass
import jakarta.validation.constraints.NotNull
import org.springframework.data.annotation.CreatedDate
import org.springframework.data.annotation.LastModifiedDate
import org.springframework.data.jpa.domain.support.AuditingEntityListener
import java.time.LocalDateTime
import java.util.*

@MappedSuperclass
@EntityListeners(AuditingEntityListener::class)
abstract class BaseEntity {

    @NotNull
    var isPublic: Boolean = true
        protected set

    @NotNull
    var isDelete: Boolean = false
        protected set

    @CreatedDate
    @Column(updatable = false)
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss", timezone = "Asia/Seoul")
    var createdAt: LocalDateTime? = null

    var createdBy: UUID? = null
        protected set

    @LastModifiedDate
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss", timezone = "Asia/Seoul")
    var updatedAt: LocalDateTime? = null

    var updatedBy: UUID? = null
        protected set

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss", timezone = "Asia/Seoul")
    var deletedAt: LocalDateTime? = null
        protected set

    var deletedBy: UUID? = null
        protected set

    fun createdBy(createdBy: UUID) {
        this.createdBy = createdBy
    }

    fun updatedBy(updatedBy: UUID) {
        this.updatedBy = updatedBy
    }

    fun deletedBy(deletedBy: UUID) {
        this.isDelete = true
        this.isPublic = false
        this.deletedBy = deletedBy
        this.deletedAt = LocalDateTime.now()
    }
}
