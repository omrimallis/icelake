//! Interface for performing schema evolution.

use std::collections::HashMap;

use crate::{IcebergResult, IcebergError};
use crate::schema::{Schema, SchemaType, StructField, PrimitiveType};

/// A set of actions that update a schema according to Iceberg schema evolution rules.
pub struct SchemaUpdate {
    /// Map of parent field ids to their respective new child fields.
    /// key=-1 indicates top-level field.
    adds: Option<HashMap<i32, Vec<StructField>>>,
    /// List of field ids to be deleted.
    deletes: Option<Vec<i32>>,
    /// Map of field ids that are updated, with their new content.
    updates: Option<HashMap<i32, StructField>>,
}

impl SchemaUpdate {
    fn new() -> Self {
        Self {
            adds: None,
            deletes: None,
            updates: None
        }
    }

    fn can_promote_primitive(base: &PrimitiveType, target: &PrimitiveType) -> bool {
        match base {
            // int can be promoted to long
            PrimitiveType::Int => {
                matches!(target, PrimitiveType::Int | PrimitiveType::Long)
            },
            // float can be promoted to double
            PrimitiveType::Float => {
                matches!(target, PrimitiveType::Float | PrimitiveType::Double)
            },
            // decimal can be promoted to decimal with higher precision
            PrimitiveType::Decimal{precision: pb, scale: sb} => {
                match target {
                    PrimitiveType::Decimal{precision: pt, scale: st} => {
                        sb == st && pt >= pb
                    },
                    _ => false
                }
            },
            _ => {
                base == target
            }
        }
    }

    fn promote_field_type(
        base: &StructField,
        target: &StructField
    ) -> IcebergResult<Option<StructField>> {
        match base.schema_type() {
            SchemaType::Primitive(pb) => {
                match target.schema_type() {
                    SchemaType::Primitive(pt) => {
                        if pb == pt {
                            Ok(None)
                        } else if Self::can_promote_primitive(pb, pt) {
                            Ok(Some(target.clone()))
                        } else {
                            Err(IcebergError::SchemaError {
                                message: format!(
                                    "can't evolve primitive field '{}' from type {} to \
                                    type {}", base.name(), pb, pt
                                )
                            })
                        }
                    },
                    _ => {
                        Err(IcebergError::SchemaError {
                            message: format!(
                                "can't evolve primitive field '{}' to non-primitive \
                                field", base.name()
                            )
                        })
                    }
                }
            },
            SchemaType::Struct(_) => {
                match target.schema_type() {
                    SchemaType::Struct(_) => { Ok(None) },
                    _ => {
                        Err(IcebergError::SchemaError {
                            message: format!(
                                "can't evolve struct field '{}' to non-struct field",
                                base.name()
                            )
                        })
                    }
                }
            },
            SchemaType::List(_) => {
                match target.schema_type() {
                    SchemaType::List(_) => { Ok(None) },
                    _ => {
                        Err(IcebergError::SchemaError {
                            message: format!(
                                "can't evolve list field '{}' to non-list field",
                                base.name()
                            )
                        })
                    }
                }
            },
            SchemaType::Map(_) => {
                match target.schema_type() {
                    SchemaType::Map(_) => { Ok(None) },
                    _ => {
                        Err(IcebergError::SchemaError {
                            message: format!(
                                "can't evolve map field '{}' to non-map field",
                                base.name()
                            )
                        })
                    }
                }
            }
        }
    }

    fn between_fields(
        parent_id: i32,
        base: &StructField,
        target: &StructField,
    ) -> IcebergResult<Self> {
        // Check if the field's type has been promoted (e.g. int -> long),
        // or if one of its properties has changed.
        let new_field = Self::promote_field_type(base, target)?
            .or_else(|| {
                if base.name() != target.name() ||
                   base.required() != target.required() ||
                   base.doc() != target.doc() {
                    Some(target.clone())
                } else {
                    None
                }
            });

        let mut schema_update = Self::new();
        if let Some(new_field) = new_field {
            if !base.required() && new_field.required() {
                return Err(IcebergError::SchemaError {
                    message: format!(
                        "can't evolve field '{}' from optional to required",
                        base.name()
                    )
                });
            }

            // The current field has changed, insert it as an update.
            schema_update.updates = Some(HashMap::from_iter([(
                (parent_id, new_field)
            )]));
        }

        // Merge recursive schema updates from nested fields.
        Ok(schema_update.merge(match base.schema_type() {
            SchemaType::Primitive(_) => { Self::new() },
            SchemaType::Struct(sb) => {
                match target.schema_type() {
                    SchemaType::Struct(st) => {
                        Self::between_field_lists(base.id(), sb.fields(), st.fields())?
                    },
                    _ => panic!()
                }
            },
            SchemaType::List(lb) => {
                match target.schema_type() {
                    SchemaType::List(lt) => {
                        Self::between_fields(base.id(), lb.field(), lt.field())?
                    },
                    _ => panic!()
                }
            },
            SchemaType::Map(mb) => {
                match target.schema_type() {
                    SchemaType::Map(mt) => {
                        Self::between_fields(base.id(), mb.key(), mt.key())?
                            .merge(
                                Self::between_fields(base.id(), mb.value(), mt.value())?
                            )
                    },
                    _ => panic!()
                }
            }
        }))
    }

    fn between_field_lists(
        parent_id: i32,
        base: &[StructField],
        target: &[StructField]
    ) -> IcebergResult<Self> {
        let base_fields: HashMap<i32, &StructField> = base
            .iter()
            .map(|field| (field.id, field))
            .collect();

        let target_fields: HashMap<i32, &StructField> = target
            .iter()
            .map(|field| (field.id, field))
            .collect();

        // Fold recursive schema updates of common fields.
        let schema_update = base_fields
            .iter()
            .filter_map(|(field_id, base_field)| {
                target_fields.get(field_id).and_then(|target_field|
                    Some((field_id, base_field, target_field))
                )
            })
            .map(|(field_id, base_field, target_field)| {
                Self::between_fields(field_id.clone(), base_field, target_field)
            })
            .collect::<IcebergResult<Vec<Self>>>()?
            .into_iter()
            .fold(Self::new(), |acc, x| acc.merge(x));

        // New fields
        let adds = target_fields
            .iter()
            .filter_map(|(id, &field)| {
                if base_fields.contains_key(id) {
                    None
                } else {
                    Some(field.clone())
                }
            })
            .collect::<Vec<StructField>>();

        let adds = if adds.is_empty() {
            None
        } else {
            Some(HashMap::from_iter([(parent_id, adds)]))
        };

        // Removed fields
        let deletes = base_fields
            .iter()
            .filter_map(|(id, _field)| {
                if target_fields.contains_key(id) {
                    None
                } else {
                    Some(id.clone())
                }
            })
            .collect::<Vec<i32>>();

        let deletes = if deletes.is_empty() {
            None
        } else {
            Some(deletes)
        };

        Ok(schema_update.merge(Self {
            adds: adds, deletes: deletes, updates: None
        }))
    }

    /// Creates a schema update that, if applied to `base`, will result in `target`.
    ///
    /// This function can also be used to check whether `base` can legally evolve to
    /// `target`.
    ///
    /// # Errors
    ///
    /// Returns [`IcebergError::SchemaError`] if `base` can't evolve to `target`
    /// according to valid iceberg schema evolution rules.
    pub fn between(base: &Schema, target: &Schema) -> IcebergResult<Self> {
        Self::between_field_lists(-1, base.fields(), target.fields())
    }

    fn merge(mut self, other: SchemaUpdate) -> Self {
        self.adds = match (self.adds, other.adds) {
            (Some(mut adds), Some(other_adds)) => {
                adds.extend(other_adds);
                Some(adds)
            },
            (opt_adds, opt_other_adds) => opt_adds.or(opt_other_adds)
        };

        self.deletes = match (self.deletes, other.deletes) {
            (Some(mut deletes), Some(other_deletes)) => {
                deletes.extend(other_deletes);
                Some(deletes)
            },
            (opt_deletes, opt_other_deletes) => opt_deletes.or(opt_other_deletes)
        };

        self.updates = match (self.updates, other.updates) {
            (Some(mut updates), Some(other_updates)) => {
                updates.extend(other_updates);
                Some(updates)
            },
            (opt_updates, opt_other_updates) => opt_updates.or(opt_other_updates)
        };

        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Test SchemaUpdate::between() when a field was added.
    #[test]
    fn between_add_field() {
        let base = Schema::new(0, vec![
            StructField::new_primitive(1, "id", true, PrimitiveType::Long)
        ]);

        let target = Schema::new(0, vec![
            StructField::new_primitive(1, "id", true, PrimitiveType::Long),
            StructField::new_primitive(2, "ts", false, PrimitiveType::Timestamp)
        ]);

        let schema_update = SchemaUpdate::between(&base, &target).unwrap();
        assert!(schema_update.updates.is_none());
        assert!(schema_update.deletes.is_none());
        assert_eq!(
            schema_update.adds.unwrap().get(&-1).unwrap(),
            &vec![
                StructField::new_primitive(2, "ts", false, PrimitiveType::Timestamp)
            ]
        );
    }

    // Test SchemaUpdate::between() when a nested struct field was added.
    #[test]
    fn between_nested_add_field() {
        let base = Schema::new(0, vec![
            StructField::new_struct(1, "name", true, vec![
                StructField::new_primitive(2, "first", false, PrimitiveType::String)
            ])
        ]);

        let target = Schema::new(0, vec![
            StructField::new_struct(1, "name", true, vec![
                StructField::new_primitive(2, "first", false, PrimitiveType::String),
                StructField::new_primitive(3, "last", false, PrimitiveType::String)
            ]),
            StructField::new_primitive(4, "ts", false, PrimitiveType::Timestamp)
        ]);

        let schema_update = SchemaUpdate::between(&base, &target).unwrap();
        assert!(schema_update.updates.is_none());
        assert!(schema_update.deletes.is_none());
        assert_eq!(
            schema_update.adds.as_ref().unwrap().get(&-1).unwrap(),
            &vec![
                StructField::new_primitive(4, "ts", false, PrimitiveType::Timestamp)
            ]
        );
        assert_eq!(
            schema_update.adds.as_ref().unwrap().get(&1).unwrap(),
            &vec![
                StructField::new_primitive(3, "last", false, PrimitiveType::String)
            ]
        );
    }

    // Test SchemaUpdate::between() when a field changed its name.
    #[test]
    fn between_field_rename() {
        let base = Schema::new(0, vec![
            StructField::new_primitive(1, "id", false, PrimitiveType::Long)
        ]);

        let target = Schema::new(0, vec![
            StructField::new_primitive(1, "user_id", false, PrimitiveType::Long)
        ]);

        let schema_update = SchemaUpdate::between(&base, &target).unwrap();
        assert!(schema_update.deletes.is_none());
        assert!(schema_update.adds.is_none());
        assert_eq!(
            schema_update.updates.as_ref().unwrap().get(&1).unwrap(),
            &StructField::new_primitive(1, "user_id", false, PrimitiveType::Long)
        );
    }

    // Test SchemaUpdate::between() when a field was deletd.
    #[test]
    fn between_field_removed() {
        let base = Schema::new(0, vec![
            StructField::new_primitive(1, "id", false, PrimitiveType::Long),
            StructField::new_primitive(2, "ts", false, PrimitiveType::Timestamp),
        ]);

        let target = Schema::new(0, vec![
            StructField::new_primitive(1, "id", false, PrimitiveType::Long),
        ]);

        let schema_update = SchemaUpdate::between(&base, &target).unwrap();
        assert!(schema_update.adds.is_none());
        assert!(schema_update.updates.is_none());
        assert!(
            schema_update.deletes.unwrap()
                .into_iter()
                .find(|id| *id == 2)
                .is_some()
        );
    }

    // Test SchemaUpdate::between() when a field type was promoted.
    #[test]
    fn between_field_promoted() {
        let base = Schema::new(0, vec![
            StructField::new_primitive(1, "id", false, PrimitiveType::Int),
        ]);
        let target = Schema::new(0, vec![
            StructField::new_primitive(1, "id", false, PrimitiveType::Long),
        ]);

        let schema_update = SchemaUpdate::between(&base, &target).unwrap();
        assert!(schema_update.adds.is_none());
        assert!(schema_update.deletes.is_none());
        assert_eq!(
            schema_update.updates.as_ref().unwrap().get(&1).unwrap(),
            &StructField::new_primitive(1, "id", false, PrimitiveType::Long)
        );
    }

    // Test SchemaUpdate::between() fails when a field type can't be promoted.
    #[test]
    fn between_field_promoted_illegaly() {
        let base = Schema::new(0, vec![
            StructField::new_primitive(1, "id", false, PrimitiveType::Long),
        ]);
        let target = Schema::new(0, vec![
            StructField::new_primitive(1, "id", false, PrimitiveType::Int),
        ]);

        assert!(matches!(
            SchemaUpdate::between(&base, &target),
            Err(IcebergError::SchemaError{..})
        ));
    }

    // Test SchemaUpdate::between() fails when a nested field type can't be promoted.
    #[test]
    fn between_nested_field_promoted_illegaly() {
        let base = Schema::new(0, vec![
            StructField::new_list(1, "items", true,
                StructField::new_primitive(2, "element", true, PrimitiveType::String)
            )
        ]);
        let target = Schema::new(0, vec![
            StructField::new_list(1, "items", true,
                StructField::new_primitive(2, "element", true, PrimitiveType::Int)
            )
        ]);

        assert!(matches!(
            SchemaUpdate::between(&base, &target),
            Err(IcebergError::SchemaError{..})
        ));
    }
}
