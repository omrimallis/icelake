//! Interface for performing iceberg schema evolution.
//!
//! # Examples
//!
//! Add a field to a schema.
//!
//! ```rust
//! use icelake::schema::{Schema, Field};
//! use icelake::schema::{SchemaType, PrimitiveType};
//! use icelake::schema::update::SchemaUpdate;
//!
//! let schema = Schema::new(0, vec![
//!     Field::new(1, "id", true, SchemaType::Primitive(PrimitiveType::Long))
//! ]);
//!
//! let new_schema_id = 1;
//! let new_schema = SchemaUpdate::for_schema(&schema)
//!     .add_field(
//!         None,
//!         "ts",
//!         false,
//!         SchemaType::Primitive(PrimitiveType::Timestamp)
//!     )
//!     .unwrap()
//!     .apply(new_schema_id);
//! ```

use std::collections::HashMap;

use crate::{IcebergResult, IcebergError};
use crate::schema::{
    Schema, Field,
    SchemaType, PrimitiveType, StructType, ListType, MapType
};

#[derive(Debug, PartialEq, Eq, Clone)]
struct FieldUpdate {
    name: String,
    required: bool,
    doc: Option<String>
}

impl FieldUpdate {
    fn new(name: &str, required: bool, doc: Option<&str>) -> Self {
        Self {
            name: name.to_string(),
            required: required,
            doc: doc.map(|s| s.to_string()),
        }
    }
}

/// A set of actions that update a schema according to Iceberg schema evolution rules.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct SchemaUpdate {
    /// Map of parent field ids to their respective new child fields.
    /// key=-1 indicates top-level field.
    adds: Option<HashMap<i32, Vec<Field>>>,
    /// List of field ids to be deleted.
    deletes: Option<Vec<i32>>,
    /// Map of field ids to their new properties.
    updates: Option<HashMap<i32, FieldUpdate>>,
    /// Map of field ids to their new type (primitive types only).
    promotions: Option<HashMap<i32, SchemaType>>,
}

impl SchemaUpdate {
    fn new() -> Self {
        Self {
            adds: None,
            deletes: None,
            updates: None,
            promotions: None
        }
    }

    /// Initializes a new [`SchemaUpdateBuilder`] helper for the given schema.
    pub fn for_schema<'a>(schema: &'a Schema) -> SchemaUpdateBuilder<'a> {
        SchemaUpdateBuilder::for_schema(schema)
    }

    fn apply_field(&self, field: &Field) -> Field {
        let update = self.updates.as_ref().and_then(|updates|
            updates.get(&field.id())
        );

        let new_type = match field.schema_type() {
            SchemaType::Primitive(_) => {
                self.promotions
                    .as_ref()
                    .and_then(|promotions|
                        promotions.get(&field.id())
                    )
                    .cloned()
                    .unwrap_or_else(||
                        field.schema_type().clone()
                    )
            },
            SchemaType::Struct(s) => {
                SchemaType::Struct(StructType::new(
                    self.apply_fields(field.id(), s.fields())
                ))
            },
            SchemaType::List(l) => {
                SchemaType::List(ListType::of_field(
                    self.apply_field(l.field())
                ))
            },
            SchemaType::Map(m) => {
                SchemaType::Map(MapType::of_fields(
                    self.apply_field(m.key()),
                    self.apply_field(m.value()),
                ))
            }
        };

        Field::new(
            field.id(),
            update.map(|u| u.name.as_str()).unwrap_or_else(|| field.name()),
            update.map(|u| u.required).unwrap_or_else(|| field.required()),
            new_type
        )
    }

    fn apply_fields(
        &self,
        parent_id: i32,
        fields: &[Field]
    ) -> Vec<Field> {
        fields.iter()
            // Remove deleted fields
            .filter(|field| {
                self.deletes
                    .as_ref()
                    .and_then(|deletes| {
                        deletes.iter().find(|deleted_field|
                            **deleted_field == field.id()
                        )
                    })
                    .is_none()
            })
            // Update remaining fields recursively
            .map(|field| self.apply_field(field))
            // Add new fields
            .chain(
                self.adds
                    .as_ref()
                    .and_then(|adds| adds.get(&parent_id))
                    .cloned()
                    .unwrap_or_default()
                    .into_iter()
            )
            .collect()
    }

    /// Applies this update to a [`Schema`], producing a new schema.
    pub fn apply(&self, new_schema_id: i32, schema: &Schema) -> Schema {
        Schema::new(new_schema_id, self.apply_fields(-1, schema.fields()))
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

    fn promote_type(
        base: &Field,
        target: &Field
    ) -> IcebergResult<Option<SchemaType>> {
        match base.schema_type() {
            SchemaType::Primitive(pb) => {
                match target.schema_type() {
                    SchemaType::Primitive(pt) => {
                        if pb == pt {
                            Ok(None)
                        } else if Self::can_promote_primitive(pb, pt) {
                            Ok(Some(target.schema_type().clone()))
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

    fn update_field(
        base: &Field,
        target: &Field
    ) -> IcebergResult<Option<FieldUpdate>> {
        if base.name() != target.name() ||
            base.required() != target.required() ||
            base.doc() != target.doc() {
            if !base.required() && target.required() {
                Err(IcebergError::SchemaError {
                    message: format!(
                        "can't evolve field '{}' from optional to required",
                        base.name()
                    )
                })
            } else {
                Ok(Some(FieldUpdate::new(
                    target.name(),
                    target.required(),
                    target.doc()
                )))
            }
        } else {
            Ok(None)
        }
    }

    fn between_fields(
        base: &Field,
        target: &Field,
    ) -> IcebergResult<Self> {
        let mut schema_update = Self::new();

        // Check if the field's type has been promoted (e.g. int -> long),
        if let Some(new_type) = Self::promote_type(base, target)? {
            schema_update.promotions
                .get_or_insert_with(HashMap::new)
                .insert(base.id(), new_type);
        }

        if let Some(field_update) = Self::update_field(base, target)? {
            schema_update.updates
                .get_or_insert_with(HashMap::new)
                .insert(base.id(), field_update);
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
                        Self::between_fields(lb.field(), lt.field())?
                    },
                    _ => panic!()
                }
            },
            SchemaType::Map(mb) => {
                match target.schema_type() {
                    SchemaType::Map(mt) => {
                        Self::between_fields(mb.key(), mt.key())?
                            .merge(
                                Self::between_fields(mb.value(), mt.value())?
                            )
                    },
                    _ => panic!()
                }
            }
        }))
    }

    fn between_field_lists(
        parent_id: i32,
        base: &[Field],
        target: &[Field]
    ) -> IcebergResult<Self> {
        let base_fields: HashMap<i32, &Field> = base
            .iter()
            .map(|field| (field.id, field))
            .collect();

        let target_fields: HashMap<i32, &Field> = target
            .iter()
            .map(|field| (field.id, field))
            .collect();

        // Fold recursive schema updates of common fields.
        let schema_update = base_fields
            .iter()
            .filter_map(|(field_id, base_field)| {
                target_fields.get(field_id).and_then(|target_field|
                    Some((base_field, target_field))
                )
            })
            .map(|(base_field, target_field)| {
                Self::between_fields(base_field, target_field)
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
            .collect::<Vec<Field>>();

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
            adds: adds, deletes: deletes, updates: None, promotions: None
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

    fn extend_opts<A, T>(opt_a: Option<T>, opt_b: Option<T>) -> Option<T>
        where T: IntoIterator<Item = A> + Extend<A>
    {
        match (opt_a, opt_b) {
            (Some(mut a), Some(b)) => {
                a.extend(b);
                Some(a)
            },
            (a, b) => a.or(b)
        }
    }

    fn merge(mut self, other: SchemaUpdate) -> Self {
        self.adds = Self::extend_opts(self.adds, other.adds);
        self.deletes = Self::extend_opts(self.deletes, other.deletes);
        self.updates = Self::extend_opts(self.updates, other.updates);
        self.promotions = Self::extend_opts(self.promotions, other.promotions);
        self
    }
}

/// Helps build [`SchemaUpdate`]s dynamically and validate the schema
/// evolution rules applied to a given schema.
pub struct SchemaUpdateBuilder<'a> {
    schema: &'a Schema,
    inner: SchemaUpdate,
    max_field_id: i32
}

impl<'a> SchemaUpdateBuilder<'a> {
    pub fn for_schema(schema: &'a Schema) -> Self {
        let max_field_id = schema.max_field_id();
        Self {
            schema: schema,
            inner: SchemaUpdate::new(),
            max_field_id: max_field_id,
        }
    }

    fn next_id(&mut self) -> i32 {
        self.max_field_id += 1;
        self.max_field_id
    }

    fn has_adds(&self, id: i32) -> bool {
        self.inner.adds
            .as_ref()
            .and_then(|adds|
                adds.keys().find(|added_id| **added_id == id)
            )
            .is_some()
    }

    fn is_promoted(&self, id: i32) -> bool {
        self.inner.promotions
            .as_ref()
            .and_then(|promotions|
                promotions.keys().find(|field_id| **field_id == id)
            )
            .is_some()
    }

    fn is_deleted(&self, id: i32) -> bool {
        self.inner.deletes
            .as_ref()
            .and_then(|deletes|
                deletes.iter().find(|delete_id| **delete_id == id)
            )
            .is_some()
    }

    pub fn add_field(
        mut self,
        parent: Option<i32>,
        name: &str,
        required: bool,
        schema_type: SchemaType
    ) -> IcebergResult<Self> {
        if let Some(parent_id) = parent {
            if self.is_deleted(parent_id) {
                return Err(IcebergError::SchemaError {
                    message: format!(
                        "can't add field '{}' to a deleted parent with id {}",
                        name, parent_id
                    )
                });
            }

            let parent = self.schema.all_fields()
                .find(|field| field.id() == parent_id)
                .ok_or_else(|| IcebergError::SchemaError {
                    message: format!(
                        "can't add field '{}' to non-existent parent id {}",
                        name, parent_id
                    )
                })?;

            if !parent.schema_type().is_struct() {
                return Err(IcebergError::SchemaError {
                    message: format!(
                        "can't add field '{}' to a non-struct parent with id {}",
                        name, parent_id
                    )
                });
            }
        }

        let field = Field::new(
            0,
            name,
            required,
            schema_type
        ).with_fresh_ids(&mut || self.next_id());

        self.inner.adds
            .get_or_insert_with(|| HashMap::new())
            .entry(parent.unwrap_or(-1))
            .or_insert_with(|| Vec::new())
            .push(field);

        Ok(self)
    }

    pub fn delete_field(mut self, field_id: i32) -> IcebergResult<Self> {
        if !self.schema.all_fields().any(|field| field.id() == field_id) {
            return Err(IcebergError::SchemaError {
                message: format!("can't delete non-existent field id {}", field_id)
            });
        }

        if self.has_adds(field_id) {
            return Err(IcebergError::SchemaError {
                message: format!(
                    "can't delete field id {} because it has added nested fields",
                    field_id
                )
            });
        }

        if self.is_promoted(field_id) {
            return Err(IcebergError::SchemaError {
                message: format!(
                    "can't delete field id {} because its type has been promoted",
                    field_id
                )
            });
        }

        self.inner.deletes
            .get_or_insert_with(Vec::new)
            .push(field_id);

        Ok(self)
    }

    pub fn promote_field(
        mut self,
        field_id: i32,
        new_type: SchemaType
    ) -> IcebergResult<Self> {
        let field = self.schema.all_fields().find(|field| field.id() == field_id)
            .ok_or_else(|| IcebergError::SchemaError {
                message: format!("can't promote non-existent field id {}", field_id)
            })?;

        if self.is_deleted(field_id) {
            return Err(IcebergError::SchemaError {
                message: format!(
                    "can't promote deleted field id {}", field_id
                )
            });
        }

        match (field.schema_type(), &new_type) {
            (SchemaType::Primitive(pb), SchemaType::Primitive(pt)) => {
                if SchemaUpdate::can_promote_primitive(pb, pt) {
                    self.inner.promotions
                        .get_or_insert_with(HashMap::new)
                        .insert(field_id, new_type);

                    Ok(self)
                } else {
                    Err(IcebergError::SchemaError {
                        message: format!(
                            "can't promote field id {} from type {} to {}",
                            field_id, pb, pt
                        )
                    })
                }
            },
            _ => {
                Err(IcebergError::SchemaError {
                    message: format!(
                        "can't promote field id {} from or to non-primitive type",
                        field_id
                    )
                })
            }
        }
    }

    /// Applies the update to the associated schema, producing a new [`Schema`] and
    /// consuming the builder.
    pub fn apply(self, new_schema_id: i32) -> Schema {
        self.inner.apply(new_schema_id, self.schema)
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    // Test SchemaUpdate::apply() when a field was added.
    #[test]
    fn add_field() {
        let base = Schema::new(0, vec![
            Field::new_primitive(1, "id", true, PrimitiveType::Long)
        ]);

        let expected = Schema::new(1, vec![
            Field::new_primitive(1, "id", true, PrimitiveType::Long),
            Field::new_primitive(2, "ts", false, PrimitiveType::Timestamp)
        ]);

        assert_eq!(
            SchemaUpdate::for_schema(&base)
                .add_field(
                    None,
                    "ts",
                    false,
                    SchemaType::Primitive(PrimitiveType::Timestamp)
                ).unwrap()
                .apply(1),
            expected
        );
    }

    #[test]
    fn delete_field() {
        let base = Schema::new(0, vec![
            Field::new_primitive(1, "id", true, PrimitiveType::Long),
            Field::new_primitive(2, "ts", false, PrimitiveType::Timestamp)
        ]);

        let expected = Schema::new(1, vec![
            Field::new_primitive(1, "id", true, PrimitiveType::Long)
        ]);

        assert_eq!(
            SchemaUpdate::for_schema(&base)
                .delete_field(2).unwrap()
                .apply(1),
            expected
        );
    }

    #[test]
    fn promote_field() {
        let base = Schema::new(0, vec![
            Field::new_primitive(1, "id", true, PrimitiveType::Int),
        ]);

        let expected = Schema::new(1, vec![
            Field::new_primitive(1, "id", true, PrimitiveType::Long)
        ]);

        assert_eq!(
            SchemaUpdate::for_schema(&base)
                .promote_field(1, SchemaType::Primitive(PrimitiveType::Long))
                .unwrap()
                .apply(1),
            expected
        );
    }

    // Test SchemaUpdate::between() when a field was added.
    #[test]
    fn between_add_field() {
        let base = Schema::new(0, vec![
            Field::new_primitive(1, "id", true, PrimitiveType::Long)
        ]);

        let target = Schema::new(0, vec![
            Field::new_primitive(1, "id", true, PrimitiveType::Long),
            Field::new_primitive(2, "ts", false, PrimitiveType::Timestamp)
        ]);

        let schema_update = SchemaUpdate::between(&base, &target).unwrap();
        assert!(schema_update.updates.is_none());
        assert!(schema_update.deletes.is_none());
        assert!(schema_update.promotions.is_none());
        assert_eq!(
            schema_update.adds.unwrap().get(&-1).unwrap(),
            &vec![
                Field::new_primitive(2, "ts", false, PrimitiveType::Timestamp)
            ]
        );
    }

    // Test SchemaUpdate::between() when a nested struct field was added.
    #[test]
    fn between_nested_add_field() {
        let base = Schema::new(0, vec![
            Field::new_struct(1, "name", true, vec![
                Field::new_primitive(2, "first", false, PrimitiveType::String)
            ])
        ]);

        let target = Schema::new(0, vec![
            Field::new_struct(1, "name", true, vec![
                Field::new_primitive(2, "first", false, PrimitiveType::String),
                Field::new_primitive(3, "last", false, PrimitiveType::String)
            ]),
            Field::new_primitive(4, "ts", false, PrimitiveType::Timestamp)
        ]);

        let schema_update = SchemaUpdate::between(&base, &target).unwrap();
        assert!(schema_update.updates.is_none());
        assert!(schema_update.deletes.is_none());
        assert!(schema_update.promotions.is_none());
        assert_eq!(
            schema_update.adds.as_ref().unwrap().get(&-1).unwrap(),
            &vec![
                Field::new_primitive(4, "ts", false, PrimitiveType::Timestamp)
            ]
        );
        assert_eq!(
            schema_update.adds.as_ref().unwrap().get(&1).unwrap(),
            &vec![
                Field::new_primitive(3, "last", false, PrimitiveType::String)
            ]
        );
    }

    // Test SchemaUpdate::between() when a field changed its name.
    #[test]
    fn between_field_rename() {
        let base = Schema::new(0, vec![
            Field::new_primitive(1, "id", false, PrimitiveType::Long)
        ]);

        let target = Schema::new(0, vec![
            Field::new_primitive(1, "user_id", false, PrimitiveType::Long)
        ]);

        let schema_update = SchemaUpdate::between(&base, &target).unwrap();
        assert!(schema_update.deletes.is_none());
        assert!(schema_update.adds.is_none());
        assert_eq!(
            schema_update.updates.as_ref().unwrap().get(&1).unwrap(),
            &FieldUpdate::new("user_id", false, None)
        );
    }

    // Test SchemaUpdate::between() when a field was deletd.
    #[test]
    fn between_field_removed() {
        let base = Schema::new(0, vec![
            Field::new_primitive(1, "id", false, PrimitiveType::Long),
            Field::new_primitive(2, "ts", false, PrimitiveType::Timestamp),
        ]);

        let target = Schema::new(0, vec![
            Field::new_primitive(1, "id", false, PrimitiveType::Long),
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
            Field::new_primitive(1, "id", false, PrimitiveType::Int),
        ]);
        let target = Schema::new(0, vec![
            Field::new_primitive(1, "id", false, PrimitiveType::Long),
        ]);

        let schema_update = SchemaUpdate::between(&base, &target).unwrap();
        assert!(schema_update.adds.is_none());
        assert!(schema_update.deletes.is_none());
        assert!(schema_update.updates.is_none());
        assert_eq!(
            schema_update.promotions.as_ref().unwrap().get(&1).unwrap(),
            &SchemaType::Primitive(PrimitiveType::Long)
        );
    }

    // Test SchemaUpdate::between() fails when a field type can't be promoted.
    #[test]
    fn between_field_promoted_illegaly() {
        let base = Schema::new(0, vec![
            Field::new_primitive(1, "id", false, PrimitiveType::Long),
        ]);
        let target = Schema::new(0, vec![
            Field::new_primitive(1, "id", false, PrimitiveType::Int),
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
            Field::new_list(1, "items", true,
                Field::new_primitive(2, "element", true, PrimitiveType::String)
            )
        ]);
        let target = Schema::new(0, vec![
            Field::new_list(1, "items", true,
                Field::new_primitive(2, "element", true, PrimitiveType::Int)
            )
        ]);

        assert!(matches!(
            SchemaUpdate::between(&base, &target),
            Err(IcebergError::SchemaError{..})
        ));
    }
}
